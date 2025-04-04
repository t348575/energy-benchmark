use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};

use common::{
    bench::{Bench, BenchArgs, BenchmarkInfo, Cmd},
    config::Settings,
    util::{find_outliers_by_stddev, parse_request_size},
};
use eyre::{Context, ContextCompat, Result, bail};
use itertools::{Itertools, iproduct};
use pyo3::{
    Bound, IntoPyObject, PyResult, Python, pyclass, pymodule,
    types::{PyAnyMethods, PyModule, PyModuleMethods},
};
use rand::{rng, seq::SliceRandom};
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use result::FioResult;
use serde::{Deserialize, Serialize};
use tokio::fs::{create_dir_all, read_to_string};
use tracing::debug;

mod result;

#[pyclass]
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
    pub num_jobs: Option<Vec<i64>>,
    pub extra_options: Option<Vec<String>>,
    pub plot: Option<Plot>,
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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Plot {
    pub variable: String,
    pub x_label: String,
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
        let cmds = iproduct!(
            self.request_sizes.iter(),
            self.io_engines.iter(),
            self.io_depths.iter(),
            jobs_vec.into_iter()
        )
        .map(|(req, eng, depth, job)| Fio {
            test_type: self.test_type.clone(),
            request_sizes: vec![req.clone()],
            io_engines: vec![eng.clone()],
            io_depths: vec![*depth],
            direct: self.direct,
            time_based: self.time_based,
            runtime: self.runtime.clone(),
            ramp_time: self.ramp_time.clone(),
            size: self.size.clone(),
            extra_options: self.extra_options.clone(),
            num_jobs: Some(vec![job]),
            plot: self.plot.clone(),
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
                for option in extra_options {
                    args.push(option.clone());
                }
            }

            if let Some(numa) = &settings.numa {
                args.push(format!("--numa_cpu_nodes={}", numa.cpunodebind));
                args.push(format!("--numa_mem_policy={}", numa.membind));
            }

            let hash = format!("{:x}", md5::compute(&args.join(" ")));
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

    fn add_path_args(&self, args: &mut Vec<String>, results_dir: &PathBuf) {
        let final_path_str = results_dir.to_str().unwrap();
        args.push(format!("--output={final_path_str}/results.json"));
        args.push(format!("--write_bw_log={final_path_str}/log"));
        args.push(format!("--write_iops_log={final_path_str}/log"));
        args.push(format!("--write_lat_log={final_path_str}/log"));
    }

    async fn check_results(&self, results_path: &PathBuf, dirs: &[String]) -> Result<Vec<usize>> {
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

        let outliers = find_outliers_by_stddev(&items, 10000.0);
        debug!("BW: {items:?}");
        Ok(outliers)
    }

    async fn plot(
        &self,
        base_path: &PathBuf,
        results_path: &PathBuf,
        info: &HashMap<String, BenchmarkInfo>,
        mut dirs: Vec<String>,
        settings: &Settings,
    ) -> Result<()> {
        dirs.shuffle(&mut rng());
        let mut groups = HashMap::new();
        for run in dirs {
            let item = info.get(&run).context("No info for run")?;
            let key = (item.name.clone(), item.power_state, item.hash.clone());
            if !groups.contains_key(&key) {
                groups.insert(key, (run, item.clone()));
            }
        }

        async fn read_results_json(folder: PathBuf) -> Result<FioResult> {
            let data = read_to_string(folder.join("results.json")).await?;
            Ok(serde_json::from_str(&data).context("Parse results.json")?)
        }

        async fn read_powersensor3(folder: PathBuf) -> Result<String> {
            Ok(read_to_string(folder.join("powersensor3.csv")).await?)
        }

        let entries = groups
            .drain()
            .map(|(_, (folder, info))| {
                (
                    read_results_json(base_path.join(folder.clone())),
                    read_powersensor3(base_path.join(folder.clone())),
                    folder,
                    info,
                )
            })
            .collect::<Vec<_>>();

        let mut ready_entries = Vec::new();
        for item in entries {
            let (json, powersensor3, folder, info) = item;
            ready_entries.push(PlotEntry {
                result: json.await?,
                folder,
                args: info.args.downcast_ref::<Fio>().unwrap().clone(),
                info,
                power: get_mean_power(&powersensor3.await?)?,
            });
        }

        let plot_path = results_path.join("plots");
        create_dir_all(&plot_path).await?;

        let throughput_dir = plot_path.join("throughput");
        create_dir_all(&throughput_dir).await?;
        self.bar_plot(
            ready_entries.clone(),
            &settings,
            throughput_dir.join(format!("{}.pdf", ready_entries[0].info.name)),
            "throughput",
            |data| {
                data.result
                    .jobs
                    .iter()
                    .map(|x| (x.read.bw_mean + x.write.bw_mean) / 1024.0)
                    .sum::<f64>()
                    / data.result.jobs.len() as f64
            },
        )?;

        let latency_dir = plot_path.join("latency");
        create_dir_all(&latency_dir).await?;
        self.bar_plot(
            ready_entries.clone(),
            &settings,
            latency_dir.join(format!("{}.pdf", ready_entries[0].info.name)),
            "latency",
            |data| {
                data.result
                    .jobs
                    .iter()
                    .map(|x| (x.read.lat_ns.mean + x.write.lat_ns.mean) / 1000000.0)
                    .sum::<f64>()
                    / data.result.jobs.len() as f64
            },
        )?;

        let power_dir = plot_path.join("power");
        create_dir_all(&power_dir).await?;
        self.bar_plot(
            ready_entries.clone(),
            &settings,
            power_dir.join(format!("{}.pdf", ready_entries[0].info.name)),
            "power",
            |data| data.power,
        )?;

        let efficiency_dir = plot_path.join("efficiency");
        create_dir_all(&efficiency_dir).await?;
        self.efficiency(ready_entries.clone(), &settings, &efficiency_dir)?;

        let bw_dir = plot_path.join("bw_over_time");
        let bw_inner_dir = bw_dir.join(&ready_entries[0].info.name);
        create_dir_all(&bw_inner_dir).await?;
        ready_entries.par_iter().for_each(|data| {
            self.bw_over_time(base_path.join(data.folder.clone()), &bw_inner_dir, &data.info)
                .expect("Error running bw_over_time");
        });
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct PlotEntry {
    result: FioResult,
    folder: String,
    info: BenchmarkInfo,
    args: Fio,
    power: f64,
}

impl Fio {
    fn efficiency(
        &self,
        ready_entries: Vec<PlotEntry>,
        settings: &Settings,
        plot_path: &PathBuf,
    ) -> Result<()> {
        if self.plot.is_none() {
            debug!("No plot for {}", ready_entries[0].info.name);
            return Ok(());
        }

        let plot = self.plot.clone().unwrap();
        let num_power_states = settings.nvme_power_states.clone().unwrap_or(vec![0]).len();

        let order = match plot.variable.as_str() {
            "request_sizes" => ready_entries
                .iter()
                .flat_map(|item| parse_request_size(&item.args.request_sizes[0]))
                .collect::<HashSet<_>>()
                .into_iter()
                .sorted()
                .enumerate()
                .map(|(x, y)| (y, x))
                .collect::<HashMap<_, _>>(),
            _ => bail!("Unsupported variable {}", plot.variable),
        };

        let mut iops_j = vec![vec![0f64; num_power_states]; order.len()];
        let mut bytes_j = iops_j.clone();

        let labels = match plot.variable.as_str() {
            "request_sizes" => ready_entries
                .iter()
                .map(|x| {
                    (
                        parse_request_size(&x.args.request_sizes[0]).unwrap(),
                        x.args.request_sizes[0].clone(),
                    )
                })
                .collect::<HashSet<_>>()
                .into_iter()
                .sorted_by(|a, b| a.0.cmp(&b.0))
                .map(|x| x.1)
                .collect::<Vec<_>>(),
            _ => bail!("Unsupported plot variable {}", plot.variable),
        };
        let experiment_name = ready_entries[0].info.name.clone();

        for item in ready_entries {
            let iops = item
                .result
                .jobs
                .iter()
                .map(|x| x.read.iops_mean + x.write.iops_mean)
                .sum::<f64>();
            let iops = iops / item.result.jobs.len() as f64;
            let bytes = item
                .result
                .jobs
                .iter()
                .map(|x| x.read.bw_mean + x.write.bw_mean)
                .sum::<f64>();
            let bytes = bytes / item.result.jobs.len() as f64;

            let x = *order
                .get(&parse_request_size(&item.args.request_sizes[0])?)
                .unwrap();
            let y = item.info.power_state as usize;
            iops_j[x][y] = (iops / 1000.0) / item.power;
            bytes_j[x][y] = (bytes / 1024.0) / item.power;
        }

        common::util::plot_python(
            |py, module| {
                let iops_j = iops_j.into_pyobject(py)?;
                let filepath = plot_path.join(format!("{}-iops-j.pdf", &experiment_name));
                module.call1((
                    filepath.to_str().unwrap(),
                    iops_j,
                    &labels,
                    &plot.x_label,
                    "KIOPS/J",
                ))?;
                Ok(())
            },
            fio,
            "fio",
            "efficiency",
        )?;

        common::util::plot_python(
            move |py, module| {
                let bytes_j = bytes_j.into_pyobject(py)?;
                let filepath = plot_path.join(format!("{}-bytes-j.pdf", &experiment_name));
                module.call1((
                    filepath.to_str().unwrap(),
                    bytes_j,
                    labels,
                    &plot.x_label,
                    "MiB/J",
                ))?;
                Ok(())
            },
            fio,
            "fio",
            "efficiency",
        )?;
        Ok(())
    }

    fn bw_over_time(
        &self,
        data_path: PathBuf,
        plot_path: &PathBuf,
        info: &BenchmarkInfo,
    ) -> Result<()> {
        if self.plot.is_none() {
            debug!("No plot for {}", info.name);
            return Ok(());
        }

        let plot = self.plot.clone().unwrap();
        let config = info.args.downcast_ref::<Fio>().unwrap();
        let variable = match plot.variable.as_str() {
            "request_sizes" => config.request_sizes[0].clone(),
            _ => bail!("Unsupported plot variable {}", plot.variable),
        };
        let name = format!("{}-{}-{}", info.name, info.power_state, variable);
        let mut child = std::process::Command::new("python3")
            .args([
                "plotting/bw_over_time.py",
                "--plot_dir",
                plot_path.to_str().unwrap(),
                "--results_dir",
                data_path.to_str().unwrap(),
                "--name",
                &name,
            ])
            .spawn()?;
        child.wait()?;
        Ok(())
    }

    fn bar_plot(
        &self,
        ready_entries: Vec<PlotEntry>,
        settings: &Settings,
        filepath: PathBuf,
        plotting_file: &str,
        get_mean: fn(&PlotEntry) -> f64,
    ) -> Result<()> {
        if self.plot.is_none() {
            debug!("No plot for {}", ready_entries[0].info.name);
            return Ok(());
        }

        let plot = self.plot.clone().unwrap();
        let num_power_states = settings.nvme_power_states.clone().unwrap_or(vec![0]).len();
        let mut results = vec![vec![]; num_power_states];
        for item in ready_entries {
            let mean = get_mean(&item);
            results[item.info.power_state as usize].push((item.args, mean));
        }

        for i in 0..num_power_states {
            results[i].sort_by(|a, b| match plot.variable.as_str() {
                "request_sizes" => parse_request_size(&a.0.request_sizes[0])
                    .unwrap()
                    .cmp(&parse_request_size(&b.0.request_sizes[0]).unwrap()),
                _ => panic!("Unsupported plot variable {}", plot.variable),
            });
        }

        let labels = match plot.variable.as_str() {
            "request_sizes" => results[0]
                .iter()
                .map(|x| x.0.request_sizes[0].clone())
                .collect::<Vec<_>>(),
            _ => bail!("Unsupported plot variable {}", plot.variable),
        };

        common::util::plot_python(
            move |py, module| {
                let results = results
                    .iter()
                    .map(|x| x.iter().map(|x| x.1).collect::<Vec<_>>())
                    .collect::<Vec<_>>();
                let data = results.into_pyobject(py)?;
                module.call1((data, filepath.to_str().unwrap(), &plot.x_label, labels))?;
                Ok(())
            },
            fio,
            "fio",
            plotting_file,
        )?;
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

#[pymodule]
fn fio(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Fio>()?;
    Ok(())
}

fn get_mean_power(data: &str) -> Result<f64> {
    let mut lines = data.lines();
    let header = lines.next().context("Missing header")?;
    let headers: Vec<&str> = header.split(',').collect();
    let total_index = headers
        .iter()
        .position(|h| *h == "Total")
        .context("Missing 'Total' column")?;

    let data_lines = lines.skip(100);

    let mut total_sum = 0.0;
    let mut count = 0;

    for line in data_lines {
        let cols: Vec<&str> = line.split(',').collect();
        if let Some(value_str) = cols.get(total_index) {
            if let Ok(value) = value_str.trim().parse::<f64>() {
                total_sum += value;
                count += 1;
            }
        }
    }
    Ok(total_sum / count as f64)
}
