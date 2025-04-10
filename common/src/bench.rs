use core::fmt::Debug;
use std::{path::Path, process::Stdio};

use downcast_rs::{Downcast, impl_downcast};
use dyn_clone::{DynClone, clone_trait_object};
use eyre::{Context, ContextCompat, Result};
use flume::Sender;
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tracing::{debug, error};

use crate::{config::Settings, sensor::SensorRequest};

#[typetag::serde(tag = "type")]
#[async_trait::async_trait]
pub trait Bench: Debug + DynClone + Downcast + Send + Sync {
    /// Benchmark program name (for identification purposes)
    fn name(&self) -> &'static str;
    /// Alias for [`Default`]
    fn default_bench() -> Box<dyn Bench>
    where
        Self: Sized + 'static;
    /// Generates the commands to run the experiment with each argument combination to test
    ///
    /// Arguments:
    /// * `settings` - Settings from config file
    /// * `bench_args` - Arguments for the experiment
    /// * `name` - Name of the experiment run
    ///
    /// Returns:
    /// * 0: Program to run
    /// * 1: [`Vec<Cmd>`] generated commands for each configuration arrangment
    fn cmds(
        &self,
        settings: &Settings,
        bench_args: &dyn BenchArgs,
        name: &str,
    ) -> Result<(String, Vec<Cmd>)>;
    /// Adds any additional arguments to arguments generated by [`Bench::cmds`] that require the final result directory: results, outputs etc.
    ///
    /// Arguments:
    /// * `args` - previously generated args by [`Bench::cmds`]
    /// * `final_results_dir` - final directory for results of run to be stored
    fn add_path_args(&self, args: &mut Vec<String>, final_results_dir: &Path);
    /// Check if results of an experiment run are OK (Check for deviations, etc.)
    ///
    /// Arguments:
    /// * `results_dir` - Directory of local run (not particular iteration)
    /// * `dirs` - List of directories to check (ie. each iteration of an experiment)
    ///
    /// Returns:
    /// * A list of index of directories (indexes corresponding to `dirs`) with results to be removed (and re-run)
    async fn check_results(&self, results_dir: &Path, dirs: &[String]) -> Result<Vec<usize>>;
    /// Default benchmark runner, override for custom logic
    ///
    /// Arguments:
    /// * `program` - Program to run
    /// * `args` - Arguments to run program with
    /// * `sensors` - Sensors that are available to record data
    /// * `final_results_dir` - Directory to store results in
    /// * `bench_copy` - Copy of the benchmark object (ie. self)
    async fn run(
        &self,
        program: &str,
        args: &[String],
        sensors: &[Sender<SensorRequest>],
        final_results_dir: &Path,
        bench_copy: Box<dyn Bench>,
    ) -> Result<()> {
        let mut child = Command::new(program)
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("Running benchmark")?;
        debug!("Benchmark started");

        for sensor in sensors {
            sensor
                .send_async(SensorRequest::StartRecording {
                    dir: final_results_dir.to_path_buf(),
                    args: args.to_vec(),
                    program: program.to_string(),
                    bench: bench_copy.clone(),
                    pid: child.id().context("Could not get benchmark process id")?,
                })
                .await?;
        }
        debug!("Sensors started");

        let status = child.wait().await?;
        debug!("Benchmark done");
        if !status.success() {
            error!("Process exitied with {}", status.code().unwrap_or_default());
        }

        for sensor in sensors {
            sensor.send_async(SensorRequest::StopRecording).await?;
        }
        debug!("Sensors stopped");
        Ok(())
    }
}
clone_trait_object!(Bench);
impl_downcast!(Bench);

#[typetag::serde(tag = "type")]
pub trait BenchArgs: Debug + DynClone + Downcast + Send + Sync {
    /// Benchmark program name, must be the same as [`Bench::name`]
    fn name(&self) -> &'static str;
}
clone_trait_object!(BenchArgs);
impl_downcast!(BenchArgs);

#[derive(Debug)]
pub struct Cmd {
    /// Arguments
    pub args: Vec<String>,
    /// Hash of arguments to use for the experiment folder name
    pub hash: String,
    /// An argument object, with only the arguments for this experiment
    pub arg_obj: Box<dyn Bench>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkInfo {
    pub power_state: i32,
    pub iteration: usize,
    /// Name of the experiment, ie. [`crate::config::Config::bench_args::name`]
    pub name: String,
    pub hash: String,
    pub args: Box<dyn Bench>,
}
