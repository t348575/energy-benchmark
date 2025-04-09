use std::path::Path;

use common::{
    bench::{Bench, BenchArgs, Cmd},
    config::Settings,
    util::{find_outliers_by_stddev, get_mean_power},
};
use eyre::{Context, ContextCompat, Result};
use itertools::iproduct;
use result::FioResult;
use serde::{Deserialize, Serialize};
use tokio::fs::read_to_string;
use tracing::debug;

pub mod result;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FioConfig {
    pub program: Option<String>,
}

#[typetag::serde]
impl BenchArgs for FioConfig {
    fn name(&self) -> &'static str {
        "fio"
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FioTestTypeConfig {
    #[serde(rename = "type")]
    pub _type: FioTestType,
    pub args: Option<FioTestTypeArgs>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FioTestTypeArgs {
    pub read: u8,
    pub write: u8,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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

    fn cmds(
        &self,
        settings: &Settings,
        bench_args: &dyn BenchArgs,
        name: &str,
    ) -> Result<(String, Vec<Cmd>)> {
        let jobs = self.num_jobs.clone();
        let jobs_vec = jobs.unwrap_or(vec![1]);
        let extra_options = self.extra_options.clone();
        let extra_options_vec = extra_options.unwrap_or(vec![vec!["--unit_base=0".to_owned()]]);
        let cmds = iproduct!(
            self.request_sizes.iter(),
            self.io_engines.iter(),
            self.io_depths.iter(),
            jobs_vec.into_iter(),
            extra_options_vec.into_iter()
        )
        .map(|(req, eng, depth, job, extra_options)| Fio {
            test_type: self.test_type.clone(),
            request_sizes: vec![req.clone()],
            io_engines: vec![eng.clone()],
            io_depths: vec![*depth],
            direct: self.direct,
            time_based: self.time_based,
            runtime: self.runtime.clone(),
            ramp_time: self.ramp_time.clone(),
            size: self.size.clone(),
            extra_options: Some(vec![extra_options]),
            num_jobs: Some(vec![job]),
        })
        .map(move |bench| {
            let mut args = vec![
                "--name",
                "--filename",
                "--direct",
                "--bs",
                "--ioengine",
                "--time_based",
                "--iodepth",
            ]
            .into_iter()
            .zip(vec![
                name.to_owned(),
                settings.device.clone(),
                int(bench.direct).to_string(),
                bench.request_sizes[0].clone(),
                bench.io_engines[0].clone(),
                int(bench.time_based).to_string(),
                bench.io_depths[0].to_string(),
            ])
            .map(|(arg, value)| format!("{arg}={value}"))
            .collect::<Vec<_>>();

            args.push("--output-format=json+".to_owned());
            args.push("--log_avg_msec=10".to_owned());
            if let Some(size) = &bench.size {
                args.push(format!("--size={size}"));
            }
            if let Some(runtime) = &bench.runtime {
                args.push(format!("--runtime={runtime}"))
            }
            if let Some(ramp_time) = &bench.ramp_time {
                args.push(format!("--ramp_time={ramp_time}"));
            }
            if let Some(jobs) = &bench.num_jobs {
                args.push(format!("--numjobs={}", jobs[0]));
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

            if let Some(numa) = &settings.numa {
                args.push(format!("--numa_cpu_nodes={}", numa.cpunodebind));
                args.push(format!("--numa_mem_policy={}", numa.membind));
            }

            let hash = format!("{:x}", md5::compute(args.join(" ")));
            Cmd {
                args,
                hash,
                arg_obj: Box::new(bench),
            }
        })
        .collect();

        let bench_args = bench_args
            .downcast_ref::<FioConfig>()
            .context("Invalid bench args, expected args for fio")?;

        Ok((bench_args.program.clone().unwrap_or("fio".to_owned()), cmds))
    }

    fn add_path_args(&self, args: &mut Vec<String>, results_dir: &Path) {
        let final_path_str = results_dir.to_str().unwrap();
        args.push(format!("--output={final_path_str}/results.json"));
        args.push(format!("--write_bw_log={final_path_str}/log"));
        args.push(format!("--write_iops_log={final_path_str}/log"));
        args.push(format!("--write_lat_log={final_path_str}/log"));
    }

    async fn check_results(&self, results_path: &Path, dirs: &[String]) -> Result<Vec<usize>> {
        let mut items = Vec::new();
        for item in dirs {
            let data: FioResult = serde_json::from_str(
                &read_to_string(results_path.join(item).join("results.json"))
                    .await
                    .context("Reading results.json")?,
            )?;
            let mean_bw = data
                .jobs
                .iter()
                .map(|x| x.read.bw_mean + x.write.bw_mean)
                .collect::<Vec<_>>();
            items.push(mean_bw.iter().sum::<f64>() / mean_bw.len() as f64);
        }

        let mut outliers = find_outliers_by_stddev(&items, 10000.0);
        debug!("BW: {items:?}");

        for (idx, item) in dirs.iter().enumerate() {
            // TODO: check if sensor exists
            let data = read_to_string(results_path.join(item).join("pmt-RAPL.csv")).await?;
            if get_mean_power(&data, "Total").is_err() && !outliers.contains(&idx){
                outliers.push(idx);
            }
        }
        Ok(outliers)
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
