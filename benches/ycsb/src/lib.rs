use std::{
    collections::HashMap,
    path::Path,
    process::Stdio,
    time::{Duration, Instant},
};

use common::{
    bench::{Bench, BenchArgs, Cmd, CmdsResult, trace_nvme_calls},
    config::{Config, Settings},
    sensor::SensorRequest,
    util::{Filesystem, mount_fs, simple_command_with_output_no_dir},
};
use eyre::{Context, ContextCompat, Result, bail};
use flume::Sender;
use result::parse_output;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, write},
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
    pub threads: Option<u32>,
    #[cfg(feature = "prefill")]
    pub prefill: Option<String>,
    pub _ycsb_op_type: Option<OpType>,
    pub fs_mount_opts: Option<String>,
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
    ) -> Result<CmdsResult> {
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

        let mut load_obj = self.clone();
        load_obj._ycsb_op_type = Some(OpType::Load);
        let mut run_obj = self.clone();
        run_obj._ycsb_op_type = Some(OpType::Run);
        let cmds = vec![
            Cmd {
                args: load,
                idx: 0,
                bench_obj: Box::new(load_obj),
            },
            Cmd {
                args: run,
                idx: 1,
                bench_obj: Box::new(run_obj),
            },
        ];

        Ok(CmdsResult { program, cmds })
    }

    async fn run(
        &self,
        program: &str,
        args: &[String],
        _env: &HashMap<String, String>,
        settings: &Settings,
        sensors: &[Sender<SensorRequest>],
        final_results_dir: &Path,
        bench_obj: Box<dyn Bench>,
        config: &Config,
        last_experiment: &Option<Box<dyn Bench>>,
    ) -> Result<()> {
        let ycsb_mount = final_results_dir.join("ycsb-mount");
        mount_fs(
            &ycsb_mount,
            &settings.device,
            &self.fs,
            !self.is_same_experiment(last_experiment)?,
            self.fs_mount_opts.clone(),
        )
        .await?;

        let marker_filename = final_results_dir.join("markers.csv");
        let mut marker_file = File::create(marker_filename).await?;
        marker_file
            .write_all("time,marker_name\n".as_bytes())
            .await?;

        #[cfg(feature = "prefill")]
        if let Some(size) = &self.prefill {
            let prefill_file = ycsb_mount.join("prefill.data");
            fio::Fio::prefill(&prefill_file, size, config, settings).await?;
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

        let mut trace = None;
        if settings.should_trace.unwrap_or(false) {
            trace.replace(trace_nvme_calls(final_results_dir).await?);
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
            simple_command_with_output_no_dir("df", &["-h", &settings.device]).await?
        );

        marker_file
            .write_all(format!("{},{}\n", start_time.elapsed().as_millis(), "unmount").as_bytes())
            .await?;
        _ = simple_command_with_output_no_dir("umount", &[&settings.device]).await?;

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
