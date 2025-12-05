use std::{collections::HashMap, path::Path};

use common::{
    bench::{Bench, BenchArgs, Cmd, CmdsResult},
    config::{Config, Settings},
    util::{
        Filesystem, get_pcie_address, mount_fs, parse_time, read_json_file,
        simple_command_with_output, simple_command_with_output_no_dir,
    },
};
use eyre::{ContextCompat, Result, bail};
use itertools::iproduct;
use serde::{Deserialize, Serialize};
use tokio::fs::{create_dir_all, read_to_string, write};
use tracing::{debug, info};

pub mod result;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fio {
    pub test_type: FioTestTypeConfig,
    pub request_sizes: Vec<String>,
    pub io_engines: Vec<String>,
    pub io_depths: Vec<usize>,
    pub direct: bool,
    pub time_based: bool,
    pub runtime: Option<String>,
    pub ramp_time: Option<String>,
    pub size: Option<String>,
    pub num_jobs: Option<Vec<usize>>,
    pub extra_options: Option<Vec<Vec<String>>>,
    pub job_specific_extra_options: Option<Vec<Vec<String>>>,
    pub job_specific_extra_options_index: Option<usize>,
    pub matched_args: Option<Vec<MatchedKv>>,
    pub fs: Option<Filesystem>,
    pub skip_format: Option<bool>,
    pub filename: Option<String>,
    pub directory: Option<String>,
    pub open_dir: Option<String>,
    // TODO: placeholder so that old config files don't break, to be removed
    pub prefill: Option<bool>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatchedKv {
    pub key: String,
    pub value: Vec<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FioConfig {
    pub program: Option<String>,
    pub log_avg: Option<usize>,
    pub spdk_path: Option<String>,
}

#[typetag::serde]
impl BenchArgs for FioConfig {
    fn name(&self) -> &'static str {
        "fio"
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FioTestTypeConfig {
    #[serde(rename = "type")]
    pub _type: FioTestType,
    pub args: Option<FioTestTypeArgs>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FioTestTypeArgs {
    pub read: u8,
    pub write: u8,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum FioTestType {
    #[default]
    Read,
    Write,
    ReadWrite,
    Randread,
    Randwrite,
    RandReadWrite,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadWriteOpts {
    pub read: i32,
    pub write: i32,
}

fn int(item: bool) -> u8 {
    if item { 1 } else { 0 }
}

#[async_trait::async_trait]
#[typetag::serde]
impl Bench for Fio {
    fn name(&self) -> &'static str {
        "fio"
    }

    fn default_bench() -> Box<dyn Bench> {
        Box::new(Self::default())
    }

    fn default_bench_args(&self) -> Box<dyn BenchArgs> {
        Box::new(FioConfig::default())
    }

    fn internal_cgroup(&self) -> bool {
        true
    }

    fn runtime_estimate(&self) -> Result<u64> {
        let runtime = parse_time(self.runtime.as_ref().unwrap_or(&"1s".to_owned()))?;
        let ramp = parse_time(self.ramp_time.as_ref().unwrap_or(&"1s".to_owned()))?;
        let total = runtime + ramp;
        Ok(total as u64)
    }

    fn cmds(
        &self,
        settings: &Settings,
        bench_args: &dyn BenchArgs,
        name: &str,
    ) -> Result<CmdsResult> {
        let bench_args = bench_args
            .downcast_ref::<FioConfig>()
            .context("Invalid bench args, expected args for fio")?;
        if self.io_engines.iter().any(|x| x.eq(&"spdk")) && bench_args.spdk_path.is_none() {
            bail!("Missing SPDK path");
        }

        let spdk = self.io_engines.iter().any(|x| x.eq(&"spdk"));
        let program = if settings.numa.is_some() && !spdk {
            "numactl".to_owned()
        } else {
            bench_args.program.clone().unwrap_or("fio".to_owned())
        };

        let jobs = self.num_jobs.clone();
        let jobs_vec = jobs.unwrap_or(vec![1]);
        if let Some(specific) = &self.job_specific_extra_options {
            if jobs_vec[0] != specific.len() {
                bail!(
                    "Number of jobs does not match number of job specific extra options, {} != {}",
                    jobs_vec[0],
                    specific.len()
                );
            }
        }

        if let Some(matched) = &self.matched_args {
            for item in matched {
                let match_str = item.key.split_once("[").unwrap();
                let requested_idx = match_str.1.trim_end_matches("]").parse::<usize>()?;
                match match_str.0 {
                    "request_sizes" => if requested_idx >= self.request_sizes.len() {
                        bail!("Matched request_sizes index out of bounds");
                    }
                    "io_engines" => if requested_idx >= self.io_engines.len() {
                        bail!("Matched io_engines index out of bounds");
                    }
                    "io_depths" => if requested_idx >= self.io_depths.len() {
                        bail!("Matched io_depths index out of bounds");
                    }
                    "num_jobs" => if requested_idx >= jobs_vec.len() {
                        bail!("Matched num_jobs index out of bounds");
                    }
                    "extra_options" => if let Some(extra_options) = &self.extra_options
                        && requested_idx >= extra_options.len()
                    {
                        bail!("Matched extra_options index out of bounds");
                    }
                    _ => bail!("Unknown matched key: {}", match_str.0),
                }
            }
        }

        let extra_options = self.extra_options.clone();
        let extra_options_vec = extra_options.unwrap_or(vec![vec!["--unit_base=0".to_owned()]]);
        let filename = self.filename.clone().unwrap_or(settings.device.clone());
        let cmds = iproduct!(
            0..self.request_sizes.len(),
            0..self.io_engines.len(),
            0..self.io_depths.len(),
            0..jobs_vec.len(),
            0..extra_options_vec.len(),
        )
        .map(|(req_idx, eng_idx, depth_idx, job_idx, extra_idx)| {
            let bench = Fio {
                test_type: self.test_type.clone(),
                request_sizes: vec![self.request_sizes[req_idx].clone()],
                io_engines: vec![self.io_engines[eng_idx].clone()],
                io_depths: vec![self.io_depths[depth_idx]],
                direct: self.direct,
                time_based: self.time_based,
                runtime: self.runtime.clone(),
                ramp_time: self.ramp_time.clone(),
                size: self.size.clone(),
                extra_options: Some(vec![extra_options_vec[extra_idx].clone()]),
                num_jobs: Some(vec![jobs_vec[job_idx]]),
                job_specific_extra_options: self.job_specific_extra_options.clone(),
                job_specific_extra_options_index: self.job_specific_extra_options_index.clone(),
                fs: self.fs.clone(),
                skip_format: self.skip_format,
                filename: if self.directory.is_some() || self.open_dir.is_some() {
                    None
                } else {
                    Some(filename.clone())
                },
                matched_args: self.matched_args.clone(),
                directory: self.directory.clone(),
                open_dir: self.open_dir.clone(),
                prefill: None
            };

            (req_idx, eng_idx, depth_idx, job_idx, extra_idx, bench)
        })
        .enumerate()
        .map(
            |(idx, (req_idx, eng_idx, depth_idx, job_idx, extra_idx, mut bench))| {
                let mut args = if !spdk && let Some(numa) = &settings.numa {
                    vec![
                        format!("--cpunodebind={}", numa.cpunodebind),
                        format!("--membind={}", numa.membind),
                        bench_args.program.clone().unwrap_or("fio".to_owned()),
                    ]
                } else {
                    Vec::new()
                };

                let temp = vec![
                    if bench.directory.is_some() {
                        "--directory"
                    } else if bench.open_dir.is_some() {
                        "--opendir"
                    } else {
                        "--filename"
                    },
                    "--direct",
                    "--bs",
                    "--ioengine",
                    "--time_based",
                    "--iodepth",
                ]
                .into_iter()
                .zip(vec![
                    if let Some(directory) = &bench.directory {
                        directory.clone()
                    } else if let Some(open_dir) = &bench.open_dir {
                        open_dir.clone()
                    } else {
                        if bench.io_engines[0].eq("spdk") {
                            let pcie_address = get_pcie_address(&settings.device)
                                .context("Get drive PCIe address")
                                .unwrap_or("00:00.0".to_owned())
                                .replace(":", ".");
                            format!("trtype=PCIe traddr={pcie_address} ns=1")
                        } else {
                            bench.filename.clone().unwrap()
                        }
                    },
                    int(bench.direct).to_string(),
                    bench.request_sizes[0].clone(),
                    bench.io_engines[0].clone(),
                    int(bench.time_based).to_string(),
                    bench.io_depths[0].to_string(),
                ])
                .map(|(arg, value)| format!("{arg}={value}"));

                args.extend(temp);
                args.push("--output-format=json+".to_owned());

                let log_avg = bench_args.log_avg.unwrap_or(10);
                if log_avg > 0 {
                    args.push(format!("--log_avg_msec={log_avg}"));
                }
                if let Some(size) = &bench.size {
                    args.push(format!("--size={size}"));
                }
                if let Some(runtime) = &bench.runtime {
                    args.push(format!("--runtime={runtime}"))
                }
                if let Some(ramp_time) = &bench.ramp_time {
                    args.push(format!("--ramp_time={ramp_time}"));
                }

                bench
                    .test_type
                    .cmds(settings)
                    .into_iter()
                    .for_each(|cmd| args.push(cmd));

                if let Some(extra_options) = &bench.extra_options {
                    for option in extra_options[0].iter() {
                        args.push(option.clone());
                    }
                }

                if let Some(matched) = &bench.matched_args {
                    let mut final_matched = Vec::new();
                    apply_matched_index(
                        "request_sizes",
                        req_idx,
                        matched,
                        &mut args,
                        &mut final_matched,
                    );
                    apply_matched_index(
                        "io_engines",
                        eng_idx,
                        matched,
                        &mut args,
                        &mut final_matched,
                    );
                    apply_matched_index(
                        "io_depths",
                        depth_idx,
                        matched,
                        &mut args,
                        &mut final_matched,
                    );
                    apply_matched_index(
                        "num_jobs",
                        job_idx,
                        matched,
                        &mut args,
                        &mut final_matched,
                    );
                    apply_matched_index(
                        "extra_options",
                        extra_idx,
                        matched,
                        &mut args,
                        &mut final_matched,
                    );
                    bench.matched_args = Some(final_matched);
                }

                if settings.cgroup.is_some() {
                    args.push("--cgroup=energy-benchmark".to_owned());
                }

                if spdk && let Some(numa) = &settings.numa {
                    args.push(format!("--numa_cpu_nodes={}", numa.cpunodebind));
                    args.push(format!("--numa_mem_policy={}", numa.membind));
                }

                if let Some(jobs) = &bench.num_jobs
                    && bench.job_specific_extra_options.is_none()
                {
                    args.push(format!("--numjobs={}", jobs[0]));
                    args.push(format!("--name={name}"));
                } else if let Some(num_jobs) = &bench.num_jobs
                    && let Some(specific) = &bench.job_specific_extra_options
                {
                    let job_specific_extra_options_index =
                        bench.job_specific_extra_options_index.unwrap_or(0);

                    for idx in 0..num_jobs[0] {
                        args.push(format!("--name={name}"));
                        if extra_idx == job_specific_extra_options_index {
                            for option in specific[idx].iter() {
                                args.push(option.clone());
                            }
                        }
                    }
                }

                Cmd {
                    args,
                    idx,
                    bench_obj: Box::new(bench),
                }
            },
        )
        .collect();

        Ok(CmdsResult { program, cmds })
    }

    fn add_path_args(&self, args: &mut Vec<String>, final_results_dir: &Path) {
        let final_path_str = final_results_dir.to_str().unwrap();
        args.push(format!("--output={final_path_str}/results.json"));
        args.push(format!("--write_bw_log={final_path_str}/log"));
        args.push(format!("--write_lat_log={final_path_str}/log"));
    }

    fn add_env(&self, bench_args: &dyn BenchArgs) -> Result<HashMap<String, String>> {
        if self.io_engines[0].eq("spdk") {
            let args = bench_args.downcast_ref::<FioConfig>().unwrap();
            let spdk_path = args.spdk_path.as_ref().context("Missing SPDK path")?;
            let spdk_path = Path::new(spdk_path);
            return Ok(HashMap::from([(
                "LD_PRELOAD".to_owned(),
                spdk_path
                    .join("build/fio/spdk_nvme")
                    .to_str()
                    .unwrap()
                    .to_owned(),
            )]));
        }
        Ok(HashMap::new())
    }

    fn requires_custom_power_state_setter(&self) -> bool {
        self.io_engines[0].eq("spdk")
    }

    fn write_hint(&self) -> bool {
        matches!(
            &self.test_type._type,
            FioTestType::Write
                | FioTestType::ReadWrite
                | FioTestType::Randwrite
                | FioTestType::RandReadWrite
        )
    }

    async fn experiment_init(
        &self,
        _data_dir: &Path,
        settings: &Settings,
        bench_args: &dyn BenchArgs,
        last_experiment: &Option<Box<dyn Bench>>,
        _config: &Config,
        _final_results_dir: &Path,
    ) -> Result<()> {
        if self.io_engines[0] == "spdk" {
            if self.fs.is_some() {
                bail!("Filesystem not supported for SPDK");
            }

            let args = bench_args.downcast_ref::<FioConfig>().unwrap();
            let spdk_dir = args.spdk_path.as_ref().context("Missing SPDK path")?;
            let spdk_dir = Path::new(spdk_dir);
            let pcie_device =
                get_pcie_address(&settings.device).context("Get drive PCIe address")?;
            write("spdk_device", &pcie_device).await?;
            let mut env = HashMap::from([("PCI_ALLOWED".to_owned(), pcie_device)]);

            if let Some(numa) = &settings.numa {
                env.insert(
                    "HUGENODE".to_owned(),
                    format!("'nodes_hp[{}]=4096", numa.cpunodebind),
                );
            } else {
                env.insert("HUGEMEM".to_owned(), "4096".to_owned());
            }

            simple_command_with_output("bash", &["./scripts/setup.sh", "config"], spdk_dir, &env)
                .await?;
            return Ok(());
        }

        let mountpoint = std::env::current_dir()?.join("mountpoint");
        if let Some(fs) = &self.fs {
            let skip_format = self.skip_format.unwrap_or(false);
            let should_format =
                !skip_format && !last_experiment_uses_same_fs(last_experiment, &fs)?;

            info!("Formatting: {should_format}");
            mount_fs(
                &mountpoint,
                &settings.device,
                fs,
                should_format,
                None::<String>,
            )
            .await?;

            if let Some(dir) = &self.directory {
                _ = create_dir_all(dir).await;
            }
        }
        Ok(())
    }

    async fn post_experiment(
        &self,
        _data_dir: &Path,
        final_results_dir: &Path,
        settings: &Settings,
        bench_args: &dyn BenchArgs,
    ) -> Result<()> {
        let results: result::FioResult =
            read_json_file(final_results_dir.join("results.json")).await?;
        let runtime = results.jobs[0].job_runtime as f64 / 1000.0;
        debug!(
            "bw_mean: ({}, {})",
            ((results.jobs.iter().map(|x| x.read.io_bytes).sum::<i64>() as f64) / 1048576.0)
                / runtime,
            ((results.jobs.iter().map(|x| x.write.io_bytes).sum::<i64>() as f64) / 1048576.0)
                / runtime
        );

        if self.io_engines[0] == "spdk" {
            let args = bench_args.downcast_ref::<FioConfig>().unwrap();
            let spdk_dir = args.spdk_path.as_ref().context("Missing SPDK path")?;
            let spdk_dir = Path::new(spdk_dir);
            let pcie_device = read_to_string("spdk_device").await?;
            simple_command_with_output(
                "bash",
                &["./scripts/setup.sh", "reset"],
                spdk_dir,
                &HashMap::from([("PCI_ALLOWED".to_owned(), pcie_device.trim().to_owned())]),
            )
            .await?;
            return Ok(());
        }

        if self.fs.is_some() {
            _ = simple_command_with_output_no_dir("umount", &[&settings.device]).await?;
        }
        Ok(())
    }
}

fn last_experiment_uses_same_fs(
    last_experiment: &Option<Box<dyn Bench>>,
    current_fs: &Filesystem,
) -> Result<bool> {
    if let Some(last_experiment) = last_experiment {
        let last_experiment = last_experiment
            .downcast_ref::<Fio>()
            .context("Invalid bench args, expected args for Fio")?;

        if last_experiment.fs.is_none() {
            return Ok(false);
        }

        return Ok(last_experiment.fs.as_ref().unwrap() == current_fs);
    }
    Ok(false)
}

impl Fio {
    pub async fn prefill(
        prefill_file: &Path,
        device: &str,
        size: &str,
        config: &Config,
        settings: &Settings,
    ) -> Result<()> {
        if prefill_file.exists() {
            return Ok(());
        }

        debug!("Creating prefill file");
        let fio = Fio {
            test_type: FioTestTypeConfig {
                _type: FioTestType::Randwrite,
                args: None,
            },
            request_sizes: vec!["4k".to_owned()],
            io_engines: vec!["io_uring".to_owned()],
            io_depths: vec![32],
            num_jobs: Some(vec![20]),
            direct: true,
            time_based: false,
            runtime: None,
            ramp_time: None,
            size: Some(size.to_owned()),
            extra_options: None,
            job_specific_extra_options: None,
            job_specific_extra_options_index: None,
            fs: None,
            skip_format: None,
            filename: Some(prefill_file.to_str().unwrap().to_owned()),
            matched_args: None,
            directory: None,
            open_dir: None,
            prefill: None
        };

        let bench_args: Box<dyn BenchArgs> = 'outer: {
            for item in &config.bench_args {
                if let Some(fio_args) = item.downcast_ref::<FioConfig>() {
                    break 'outer Box::new(fio_args.clone());
                }
            }
            fio.default_bench_args()
        };
        let mut prefill_settings = settings.clone();
        prefill_settings.device = device.to_owned();
        prefill_settings.numa = None;
        prefill_settings.nvme_power_states = None;
        let CmdsResult { cmds, program } = fio.cmds(&prefill_settings, &*bench_args, "prefill")?;
        let args = cmds[0].args.iter().map(|x| x.as_str()).collect::<Vec<_>>();
        _ = simple_command_with_output_no_dir(&program, &args).await?;
        Ok(())
    }
}

impl FioTestTypeConfig {
    fn cmds(&self, _: &Settings) -> Vec<String> {
        let mut cmds = match self._type {
            FioTestType::Read => vec!["read".to_owned()],
            FioTestType::Write => vec!["write".to_owned()],
            FioTestType::ReadWrite => vec![
                "rw".to_owned(),
                format!("--rwmixread={}", self.args.as_ref().unwrap().read),
                format!("--rwmixwrite={}", self.args.as_ref().unwrap().write),
            ],
            FioTestType::Randread => vec!["randread".to_owned()],
            FioTestType::Randwrite => vec!["randwrite".to_owned()],
            FioTestType::RandReadWrite => vec![
                "randrw".to_owned(),
                format!("--rwmixread={}", self.args.as_ref().unwrap().read),
                format!("--rwmixwrite={}", self.args.as_ref().unwrap().write),
            ],
        };
        cmds[0] = format!("--rw={}", cmds[0]);
        cmds
    }
}

fn apply_matched_index(
    field: &str,
    index: usize,
    matched: &Vec<MatchedKv>,
    args: &mut Vec<String>,
    final_matched: &mut Vec<MatchedKv>,
) {
    let key = format!("{field}[{index}]");
    if let Some(extra_args) = matched.iter().find(|x| x.key == key) {
        final_matched.push(extra_args.clone());
        for a in &extra_args.value {
            args.push(a.clone());
        }
    }
}
