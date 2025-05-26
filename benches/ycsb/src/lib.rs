use std::{
    collections::HashMap,
    path::Path,
    process::Stdio,
    time::{Duration, Instant},
};

use common::{
    bench::{Bench, BenchArgs, Cmd, trace_nvme_calls},
    config::{Config, Settings},
    sensor::SensorRequest,
    util::{CommandError, Filesystem, simple_command_with_output},
};
use eyre::{Context, ContextCompat, Result, bail};
use flume::Sender;
use result::parse_output;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, create_dir_all, write},
    io::AsyncWriteExt,
    process::Command,
    time::sleep,
};
use tracing::debug;

pub mod result;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Ycsb {
    pub workload_file: String,
    pub data_var_name: String,
    pub vars: Option<HashMap<String, String>>,
    pub db: String,
    pub fs: Filesystem,
    pub trace: Option<bool>,
    pub threads: Option<u32>,
    #[cfg(feature = "prefill")]
    pub prefill: Option<String>,
    pub _ycsb_op_type: Option<OpType>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum OpType {
    #[default]
    None,
    Load,
    Run,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct YcsbConfig {
    pub root_dir: String,
}

#[typetag::serde]
impl BenchArgs for YcsbConfig {
    fn name(&self) -> &'static str {
        "ycsb"
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl Bench for Ycsb {
    fn name(&self) -> &'static str {
        "ycsb"
    }

    fn default_bench() -> Box<dyn Bench> {
        Box::new(Self::default())
    }

    fn default_bench_args(&self) -> Box<dyn BenchArgs> {
        Box::new(YcsbConfig::default())
    }

    fn runtime_estimate(&self) -> Result<u64> {
        Ok(0)
    }

    fn cmds(
        &self,
        settings: &Settings,
        bench_args: &dyn BenchArgs,
        _name: &str,
    ) -> Result<(String, Vec<Cmd>)> {
        let _bench_args = bench_args
            .downcast_ref::<YcsbConfig>()
            .context("Invalid bench args, expected args for Ycsb")?;
        let ycsb_program = "bin/ycsb.sh".to_owned();
        let (program, is_numa) = if settings.numa.is_some() {
            ("numactl".to_owned(), true)
        } else {
            (ycsb_program.clone(), false)
        };

        let mut args = Vec::new();
        if is_numa {
            let numa = settings.numa.as_ref().unwrap();
            args.append(&mut vec![
                format!("--cpunodebind={}", numa.cpunodebind),
                format!("--membind={}", numa.membind),
                ycsb_program,
            ]);
        }

        let mut continued_args = vec![self.db.clone(), "-P".to_owned(), self.workload_file.clone()];

        if let Some(threads) = self.threads {
            continued_args.push("-threads".to_owned());
            continued_args.push(threads.to_string());
        }

        for (k, v) in self.vars.as_ref().unwrap_or(&HashMap::new()).iter() {
            continued_args.push("-p".to_owned());
            continued_args.push(format!("{k}={v}"));
        }

        let mut load = args.clone();
        load.push("load".to_owned());
        load.extend(continued_args.clone());

        let mut run = args.clone();
        run.push("run".to_owned());
        run.extend(continued_args.clone());

        let hash_load = format!("{:x}", md5::compute(load.join(" ")));
        let hash_run = format!("{:x}", md5::compute(run.join(" ")));
        let mut load_obj = self.clone();
        load_obj._ycsb_op_type = Some(OpType::Load);
        let mut run_obj = self.clone();
        run_obj._ycsb_op_type = Some(OpType::Run);
        let cmds = vec![
            Cmd {
                args: load,
                hash: hash_load,
                arg_obj: Box::new(load_obj),
            },
            Cmd {
                args: run,
                hash: hash_run,
                arg_obj: Box::new(run_obj),
            },
        ];

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
        config: &Config,
        last_experiment: &Option<Box<dyn Bench>>,
    ) -> Result<()> {
        let ycsb_mount = final_results_dir.join("ycsb-mount");
        _ = create_dir_all(&ycsb_mount).await?;

        let marker_filename = final_results_dir.join("markers.csv");
        let mut marker_file = File::create(marker_filename).await?;
        marker_file
            .write_all("time,marker_name\n".as_bytes())
            .await?;

        let ycsb_mount_str = ycsb_mount.to_str().unwrap();
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

        if !self.is_same_experiment(last_experiment)? {
            _ = simple_command_with_output("bash", &["-c", &self.fs.cmd(&settings.device)?])
                .await?;
            debug!("Created filesystem");
        }
        _ = simple_command_with_output("mount", &[&settings.device, ycsb_mount_str]).await?;

        #[cfg(feature = "prefill")]
        if let Some(prefill) = &self.prefill {
            use fio::*;
            let prefill_file = ycsb_mount.join("prefill.data");
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
                    for item in &config.bench_args {
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

        let bench_args = 'inner: {
            for args in &config.bench_args {
                if args.name() == self.name() {
                    break 'inner args.clone();
                }
            }
            bail!(
                "Could not find bench args for bench ycsb! Root directory of YCSB installation must be provided."
            );
        };
        let ycsb_args = bench_args
            .downcast_ref::<YcsbConfig>()
            .context("Not valid Ycsb config")?;

        let should_trace = self.trace.unwrap_or(false);
        let mut trace = None;
        if should_trace {
            trace.replace(trace_nvme_calls(&final_results_dir, &settings).await?);
        }

        let child = Command::new(program)
            .args(args)
            .arg("-p")
            .arg(format!(
                "{}={}",
                self.data_var_name,
                ycsb_mount.canonicalize().unwrap().to_str().unwrap()
            ))
            .current_dir(&ycsb_args.root_dir)
            .envs(settings.env.as_ref().unwrap_or(&HashMap::new()))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("Running benchmark")?;
        debug!("Benchmark started");

        let start_time = Instant::now();
        for sensor in sensors {
            sensor
                .send_async(SensorRequest::StartRecording {
                    dir: final_results_dir.to_path_buf(),
                    args: args.to_vec(),
                    program: program.to_string(),
                    bench: bench_obj.clone(),
                    pid: child.id().context("Could not get benchmark process id")?,
                })
                .await?;
        }
        debug!("Sensors started");

        let output = child.wait_with_output().await?;
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

        if !output.status.success() {
            bail!(
                "Process exitied with {}, err: {}",
                output.status.code().unwrap_or_default(),
                String::from_utf8(output.stderr)?
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

        let stdout = String::from_utf8(output.stdout)?;
        write(final_results_dir.join("output.txt"), &stdout).await?;
        let data = match parse_output(&stdout) {
            Ok(s) => s,
            Err(err) => {
                bail!(
                    "Failed to parse filebench output! err: {} stdout: {} stderr: {}",
                    err,
                    stdout,
                    String::from_utf8(output.stderr)?
                );
            }
        };
        write(
            final_results_dir.join("results.json"),
            serde_json::to_string(&data)?,
        )
        .await?;
        Ok(())
    }
}

impl Ycsb {
    fn is_same_experiment(&self, last_experiment: &Option<Box<dyn Bench>>) -> Result<bool> {
        if let Some(last_experiment) = last_experiment {
            let last_experiment = last_experiment
                .downcast_ref::<Ycsb>()
                .context("Invalid bench args, expected args for ycsb")?;

            return Ok(last_experiment.workload_file == self.workload_file
                && last_experiment
                    ._ycsb_op_type
                    .as_ref()
                    .context("Ycsb op type not set")?
                    .eq(&OpType::Load)
                    == self
                        ._ycsb_op_type
                        .as_ref()
                        .context("Ycsb op type not set")?
                        .eq(&OpType::Run));
        }
        Ok(false)
    }
}
