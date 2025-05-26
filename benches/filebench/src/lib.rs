use std::{
    collections::HashMap,
    path::Path,
    pin::Pin,
    process::Stdio,
    time::{Duration, Instant},
};

use common::{
    bench::{Bench, BenchArgs, Cmd, trace_nvme_calls},
    config::{Config, Settings},
    sensor::SensorRequest,
    util::{CommandError, Filesystem, read_until_prompt, simple_command_with_output},
};
use eyre::{Context, ContextCompat, Result, bail};
use flume::Sender;
use itertools::iproduct;
use result::{FilebenchSummary, parse_output};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, create_dir_all, write},
    io::{AsyncReadExt, AsyncWriteExt},
    join,
    process::Command,
    time::sleep,
};
use tracing::debug;

pub mod result;

const FILEBENCH_PROMPT: &str = "filebench>";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Filebench {
    pub job_file: String,
    pub vars: Option<Vec<HashMap<String, String>>>,
    pub runtime: usize,
    pub fs: Vec<Filesystem>,
    pub trace: Option<bool>,
    #[cfg(feature = "prefill")]
    pub prefill: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FilebenchConfig {
    pub program: Option<String>,
}

#[typetag::serde]
impl BenchArgs for FilebenchConfig {
    fn name(&self) -> &'static str {
        "filebench"
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl Bench for Filebench {
    fn name(&self) -> &'static str {
        "filebench"
    }

    fn default_bench() -> Box<dyn Bench> {
        Box::new(Self::default())
    }

    fn default_bench_args(&self) -> Box<dyn BenchArgs> {
        Box::new(FilebenchConfig::default())
    }

    fn runtime_estimate(&self) -> Result<u64> {
        let f = self.fs.len() as u64;
        let vars = self.vars.as_ref().unwrap_or(&vec![HashMap::new()]).len() as u64;
        let runtime = self.runtime as u64;
        Ok(f * vars * runtime)
    }

    fn cmds(
        &self,
        settings: &Settings,
        bench_args: &dyn BenchArgs,
        _name: &str,
    ) -> Result<(String, Vec<Cmd>)> {
        let vars = self.vars.clone();
        let vars_vec = vars.unwrap_or(vec![HashMap::new()]);

        let bench_args = bench_args
            .downcast_ref::<FilebenchConfig>()
            .context("Invalid bench args, expected args for Filebench")?;
        let filebench_program = bench_args.program.clone().unwrap_or("filebench".to_owned());
        let (program, is_numa) = if settings.numa.is_some() {
            ("numactl".to_owned(), true)
        } else {
            (filebench_program.clone(), false)
        };

        let cmds = iproduct!(self.fs.clone().into_iter(), vars_vec.into_iter())
            .map(|(fs, vars)| Filebench {
                job_file: self.job_file.clone(),
                vars: Some(vec![vars]),
                runtime: self.runtime,
                fs: vec![fs],
                trace: self.trace.clone(),
                #[cfg(feature = "prefill")]
                prefill: self.prefill.clone(),
            })
            .map(|bench| {
                let mut args = Vec::new();
                if is_numa {
                    let numa = settings.numa.as_ref().unwrap();
                    args.append(&mut vec![
                        format!("--cpunodebind={}", numa.cpunodebind),
                        format!("--membind={}", numa.membind),
                        filebench_program.clone(),
                    ]);
                }

                let mut hash_args = args.clone();
                hash_args.push(self.job_file.clone());
                hash_args.extend(
                    bench.vars.as_ref().unwrap()[0]
                        .iter()
                        .map(|x| format!("{}={}", x.0, x.1)),
                );
                hash_args.push(format!("{:?}", bench.fs[0]));
                hash_args.push(bench.runtime.to_string());

                let hash = format!("{:x}", md5::compute(hash_args.join(" ")));
                Cmd {
                    args,
                    hash,
                    arg_obj: Box::new(bench),
                }
            })
            .collect();

        Ok((program, cmds))
    }

    fn add_path_args(&self, _args: &mut Vec<String>, _results_dir: &Path) {}

    async fn check_results(&self, _results_path: &Path, _dirs: &[String]) -> Result<Vec<usize>> {
        Ok(vec![])
    }

    async fn run(
        &self,
        program: &str,
        args: &[String],
        settings: &Settings,
        sensors: &[Sender<SensorRequest>],
        final_results_dir: &Path,
        bench_obj: Box<dyn Bench>,
        _config: &Config,
        last_experiment: &Option<Box<dyn Bench>>,
    ) -> Result<()> {
        let filebench_mount = final_results_dir.join("filebench-mount");
        _ = create_dir_all(&filebench_mount).await?;

        let marker_filename = final_results_dir.join("markers.csv");
        let mut marker_file = File::create(marker_filename).await?;
        marker_file
            .write_all("time,marker_name\n".as_bytes())
            .await?;

        let filebench_mount_str = filebench_mount.to_str().unwrap();

        if let Err(err) = simple_command_with_output("umount", &[&settings.device]).await {
            match &err {
                CommandError::RunError { stderr, .. } => {
                    if !stderr.contains(": not mounted.") {
                        bail!(err);
                    }
                }
                _ => {
                    bail!(err);
                }
            }
        }

        if !self.fs.is_empty() && !last_experiment_uses_same_fs(last_experiment, &self.fs[0])? {
            _ = simple_command_with_output("bash", &["-c", &self.fs[0].cmd(&settings.device)?])
                .await?;
        }
        _ = simple_command_with_output("mount", &[&settings.device, filebench_mount_str]).await?;

        #[cfg(feature = "prefill")]
        if let Some(prefill) = &self.prefill {
            use fio::*;
            let prefill_file = filebench_mount.join("prefill.data");
            if !prefill_file.exists() {
                debug!("Creating prefill file");
                let fio = Fio {
                    test_type: FioTestTypeConfig {
                        _type: FioTestType::Write,
                        args: None,
                    },
                    request_sizes: vec!["4M".to_owned()],
                    io_engines: vec!["io_uring".to_owned()],
                    io_depths: vec![256],
                    direct: true,
                    time_based: false,
                    runtime: None,
                    ramp_time: None,
                    size: Some(prefill.clone()),
                    extra_options: None,
                    num_jobs: None,
                };

                let bench_args: Box<dyn BenchArgs> = 'outer: {
                    for item in &_config.bench_args {
                        if let Some(fio_args) = item.downcast_ref::<FioConfig>() {
                            break 'outer Box::new(fio_args.clone());
                        }
                    }
                    self.default_bench_args()
                };
                let mut prefill_settings = settings.clone();
                prefill_settings.device = prefill_file.to_str().unwrap().to_owned();
                prefill_settings.numa = None;
                prefill_settings.nvme_power_states = None;
                let (program, args) = fio.cmds(&prefill_settings, &*bench_args, "prefill")?;
                let args = args[0].args.iter().map(|x| x.as_str()).collect::<Vec<_>>();
                _ = simple_command_with_output(&program, &args).await?;
            }
        }

        let mut filebench = Command::new(program)
            .args(args)
            .envs(settings.env.as_ref().unwrap_or(&HashMap::new()))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("Running benchmark")?;

        let mut filebench_stdin = filebench.stdin.take().context("Could not take stdin")?;
        let mut filebench_stdout = filebench.stdout.take().context("Could not take stdout")?;
        let mut filebench_stderr = filebench.stderr.take().context("Could not take stderr")?;

        let mut stdin = Pin::new(&mut filebench_stdin);
        let mut stdout = Pin::new(&mut filebench_stdout);
        let stderr = Pin::new(&mut filebench_stderr);

        let should_trace = self.trace.unwrap_or(false);
        let mut trace = None;
        if should_trace {
            trace.replace(trace_nvme_calls(&final_results_dir, &settings).await?);
        }

        read_until_prompt(&mut stdout, FILEBENCH_PROMPT).await?;
        send_filebench_cmd(&mut stdin, &mut stdout, &format!("load {}", self.job_file)).await?;

        for (k, v) in self.vars.as_ref().unwrap()[0].iter() {
            send_filebench_cmd(&mut stdin, &mut stdout, &format!("set ${k}={v}")).await?;
        }

        send_filebench_cmd(
            &mut stdin,
            &mut stdout,
            &format!("set $dir={filebench_mount_str}"),
        )
        .await?;
        let start_time = Instant::now();
        for sensor in sensors {
            sensor
                .send_async(SensorRequest::StartRecording {
                    dir: final_results_dir.to_path_buf(),
                    args: args.to_vec(),
                    program: program.to_string(),
                    bench: bench_obj.clone(),
                    pid: filebench
                        .id()
                        .context("Could not get benchmark process id")?,
                })
                .await?;
        }
        if let Some(trace) = &trace {
            write(
                final_results_dir.join("trace_offset"),
                trace.2.elapsed().as_millis().to_string(),
            )
            .await?
        }
        debug!("Sensors started");
        send_filebench_cmd(&mut stdin, &mut stdout, "create fileset").await?;
        send_filebench_cmd(&mut stdin, &mut stdout, "system \"sync\"").await?;
        send_filebench_cmd(
            &mut stdin,
            &mut stdout,
            "system \"echo 3 > /proc/sys/vm/drop_caches\"",
        )
        .await?;

        debug!("Fileset created");
        marker_file
            .write_all(
                format!(
                    "{},{}\n",
                    start_time.elapsed().as_millis(),
                    "create-fileset"
                )
                .as_bytes(),
            )
            .await?;

        stdin
            .write_all(format!("run {}", self.runtime).as_bytes())
            .await?;
        stdin.write_all(b"\n").await?;
        stdin.flush().await?;
        let (stdout, stderr, exit_status) =
            join!(read_output(stdout), read_output(stderr), filebench.wait());
        let exit_status = exit_status?;
        let stdout = stdout?;
        let stderr = stderr?;
        debug!("Benchmark done");

        marker_file
            .write_all(
                format!(
                    "{},{}\n",
                    start_time.elapsed().as_millis(),
                    "benchmark-done"
                )
                .as_bytes(),
            )
            .await?;

        if !exit_status.success() {
            bail!(
                "Process exitied with {}, err: {}",
                exit_status.code().unwrap_or_default(),
                stderr
            );
        }

        sleep(Duration::from_secs(60)).await;
        debug!(
            "Disk sizes: {}",
            simple_command_with_output("df", &["-h", &settings.device]).await?
        );

        marker_file
            .write_all(format!("{},{}\n", start_time.elapsed().as_millis(), "unmount").as_bytes())
            .await?;
        _ = simple_command_with_output("umount", &[&settings.device]).await?;

        for sensor in sensors {
            sensor.send_async(SensorRequest::StopRecording).await?;
        }
        debug!("Sensors stopped");

        if let Some(mut trace) = trace {
            trace.0.kill().await?;
            trace.1.await?;
        }

        write(final_results_dir.join("output.txt"), &stdout).await?;
        let (summary, ops_stats) = match parse_output(&stdout) {
            Ok(s) => s,
            Err(err) => bail!(
                "Failed to parse filebench output! err: {} stdout: {} stderr: {}",
                err,
                stdout,
                stderr
            ),
        };
        write(
            final_results_dir.join("results.json"),
            serde_json::to_string(&FilebenchSummary { summary, ops_stats })?,
        )
        .await?;
        Ok(())
    }
}

fn last_experiment_uses_same_fs(
    last_experiment: &Option<Box<dyn Bench>>,
    current_fs: &Filesystem,
) -> Result<bool> {
    if let Some(last_experiment) = last_experiment {
        let last_experiment = last_experiment
            .downcast_ref::<Filebench>()
            .context("Invalid bench args, expected args for Filebench")?;

        return Ok(last_experiment.fs[0] == *current_fs);
    }
    Ok(false)
}

async fn send_filebench_cmd(
    stdin: &mut Pin<&mut impl AsyncWriteExt>,
    reader: &mut Pin<&mut impl AsyncReadExt>,
    cmd: &str,
) -> Result<String> {
    stdin.write_all(cmd.as_bytes()).await?;
    stdin.write_all(b"\n").await?;
    stdin.flush().await?;

    read_until_prompt(reader, FILEBENCH_PROMPT).await
}

async fn read_output(mut stdout: Pin<&mut impl AsyncReadExt>) -> Result<String> {
    let mut buffer = Vec::new();
    loop {
        let mut buf = [0u8; 1024];
        let n = stdout.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        buffer.extend_from_slice(&buf);
    }
    Ok(String::from_utf8(buffer)?)
}
