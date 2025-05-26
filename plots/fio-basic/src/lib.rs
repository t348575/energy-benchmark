use std::{
    collections::{HashMap, HashSet},
    fs::write,
    path::{Path, PathBuf},
};

use common::{
    bench::BenchmarkInfo,
    config::{Config, Settings},
    plot::{Plot, PlotType},
    util::{get_mean_power, parse_data_size, plot_python},
};
use eyre::{Context, ContextCompat, Result, bail};
use fio::{
    Fio,
    result::{FioResult, Job},
};
use futures::future::join_all;
use itertools::Itertools;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::fs::{create_dir_all, read_to_string};
use tracing::debug;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FioBasic {
    pub variable: String,
    pub x_label: String,
    pub group: Option<Group>,
    pub labels: Option<Vec<String>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Group {
    pub filter: String,
    pub name: String,
    pub x_label: String,
}

#[derive(Debug, Clone)]
struct PlotEntry {
    result: FioResult,
    info: BenchmarkInfo,
    args: Fio,
    ssd_power: f64,
    cpu_power: f64,
    dram_power: f64,
    node_0_power: f64,
    node_1_power: f64,
    plot: FioBasic,
}

#[async_trait::async_trait]
#[typetag::serde]
impl Plot for FioBasic {
    fn required_sensors(&self) -> &'static [&'static str] {
        &["Powersensor3", "Rapl"]
    }

    async fn plot(
        &self,
        plot_type: &PlotType,
        data_path: &Path,
        plot_path: &Path,
        config_yaml: &Config,
        info: &HashMap<String, BenchmarkInfo>,
        dirs: Vec<String>,
        settings: &Settings,
        completed_dirs: &mut Vec<String>,
    ) -> Result<()> {
        let dirs = match &self.group {
            Some(group) => {
                if *plot_type == PlotType::Individual {
                    return Ok(());
                }
                let r = Regex::new(&group.filter)?;
                dirs.into_iter()
                    .filter(|x| {
                        r.is_match(&info.get(x).unwrap().name) && !completed_dirs.contains(x)
                    })
                    .collect()
            }
            None => {
                if *plot_type == PlotType::Total {
                    return Ok(());
                }
                dirs
            }
        };

        debug!("Got {} dirs", dirs.len());
        let mut groups = HashMap::new();
        for run in dirs {
            let item = info.get(&run).context("No info for run")?;
            let key = (item.name.clone(), item.power_state, item.hash.clone());
            if !groups.contains_key(&key) {
                let plots = &config_yaml
                    .benches
                    .iter()
                    .find(|x| x.name.eq(&item.name))
                    .context("No config for run")?
                    .plots;
                let plot_obj = plots
                    .as_ref()
                    .context("No plots for run")?
                    .iter()
                    .find(|x| x.is::<FioBasic>())
                    .unwrap()
                    .downcast_ref::<FioBasic>()
                    .unwrap();
                completed_dirs.push(run.clone());
                groups.insert(key, (run, item.clone(), plot_obj));
            }
        }

        if groups.is_empty() {
            return Ok(());
        }

        async fn read_results_json(folder: PathBuf) -> Result<FioResult> {
            let data = read_to_string(folder.join("results.json")).await?;
            serde_json::from_str(&data).context("Parse results.json")
        }

        async fn read_powersensor3(folder: PathBuf) -> Result<String> {
            Ok(read_to_string(folder.join("powersensor3.csv")).await?)
        }

        async fn read_rapl(folder: PathBuf) -> Result<String> {
            Ok(read_to_string(folder.join("rapl.csv")).await?)
        }

        let entries = groups
            .drain()
            .map(|(_, (folder, info, plot))| async {
                let results = read_results_json(data_path.join(folder.clone())).await;
                let ps3 = read_powersensor3(data_path.join(folder.clone())).await;
                let rapl = read_rapl(data_path.join(folder.clone())).await;
                (results, ps3, rapl, folder, info, plot.clone())
            })
            .collect::<Vec<_>>();

        let entries = join_all(entries).await;
        let mut ready_entries = Vec::new();
        for item in entries {
            let (json, powersensor3, rapl, _, info, plot) = item;
            let rapl = rapl?;
            ready_entries.push(PlotEntry {
                result: json?,
                args: info.args.downcast_ref::<Fio>().unwrap().clone(),
                info,
                ssd_power: get_mean_power(&powersensor3?, "Total")?,
                cpu_power: get_mean_power(&rapl, "Total")?,
                dram_power: get_mean_power(&rapl, "dram-1")?,
                node_0_power: get_mean_power(&rapl, "package-0")?,
                node_1_power: get_mean_power(&rapl, "package-1")?,
                plot: plot.clone(),
            });
        }

        let experiment_name = match &self.group {
            Some(group) => group.name.clone(),
            None => ready_entries[0].info.name.clone(),
        };

        let throughput_dir = plot_path.join("throughput");
        let latency_dir = plot_path.join("latency");
        let power_dir = plot_path.join("power");
        let efficiency_dir = plot_path.join("efficiency");
        let dirs = [&throughput_dir, &latency_dir, &power_dir, &efficiency_dir];
        for dir in join_all(dirs.iter().map(create_dir_all)).await.into_iter() {
            dir?;
        }

        let mut plot_jobs: Vec<(
            Vec<PlotEntry>,
            &Settings,
            PathBuf,
            &str,
            Option<&str>,
            fn(&PlotEntry) -> f64,
        )> = Vec::new();
        plot_jobs.push((
            ready_entries.clone(),
            settings,
            throughput_dir.join(format!("{}.pdf", experiment_name)),
            "throughput",
            None,
            |data| {
                data.result
                    .jobs
                    .iter()
                    .map(|x| (x.read.bw_mean + x.write.bw_mean) / 1024.0)
                    .sum::<f64>()
            },
        ));
        plot_jobs.push((
            ready_entries.clone(),
            settings,
            latency_dir.join(format!("{}.pdf", experiment_name)),
            "latency",
            None,
            |data| data.result.jobs.iter().map(mean_latency).sum::<f64>(),
        ));
        plot_jobs.push((
            ready_entries.clone(),
            settings,
            latency_dir.join(format!("{}-p99.pdf", experiment_name)),
            "latency",
            None,
            |data| data.result.jobs.iter().map(mean_p99_latency).sum::<f64>(),
        ));
        plot_jobs.push((
            ready_entries.clone(),
            settings,
            power_dir.join(format!("{}-ssd.pdf", experiment_name)),
            "power",
            Some("SSD"),
            |data| data.ssd_power,
        ));
        plot_jobs.push((
            ready_entries.clone(),
            settings,
            power_dir.join(format!("{}-cpu.pdf", experiment_name)),
            "power",
            Some("CPU + DRAM"),
            |data| data.cpu_power,
        ));
        plot_jobs.push((
            ready_entries.clone(),
            settings,
            power_dir.join(format!("{}-dram.pdf", experiment_name)),
            "power",
            Some("DRAM 1"),
            |data| data.dram_power,
        ));
        plot_jobs.push((
            ready_entries.clone(),
            settings,
            power_dir.join(format!("{}-node-0.pdf", experiment_name)),
            "power",
            Some("Node 0"),
            |data| data.node_0_power,
        ));
        plot_jobs.push((
            ready_entries.clone(),
            settings,
            power_dir.join(format!("{}-node-1.pdf", experiment_name)),
            "power",
            Some("Node 1"),
            |data| data.node_1_power,
        ));

        let results = plot_jobs
            .into_par_iter()
            .map(|x| self.bar_plot(x.0, x.1, x.2, x.3, x.4, x.5))
            .collect::<Vec<_>>();
        for item in results {
            item?;
        }

        let efficiency_dir = plot_path.join("efficiency");
        create_dir_all(&efficiency_dir).await?;
        self.efficiency(ready_entries.clone(), settings, &efficiency_dir)
            .await?;

        Ok(())
    }
}

impl FioBasic {
    async fn efficiency(
        &self,
        ready_entries: Vec<PlotEntry>,
        settings: &Settings,
        plot_path: &Path,
    ) -> Result<()> {
        let num_power_states = settings.nvme_power_states.clone().unwrap_or(vec![0]).len();
        let (order, labels) = self.get_order_labels(&ready_entries)?;
        let mut iops_j = vec![vec![0f64; num_power_states]; order.len()];
        let mut iops_j_cpu = iops_j.clone();
        let mut edp = iops_j.clone();
        let mut edp_p99 = iops_j.clone();
        let mut bytes_j = iops_j.clone();
        let mut bytes_j_cpu = iops_j.clone();
        let experiment_name = match &self.group {
            Some(group) => group.name.clone(),
            None => ready_entries[0].info.name.clone(),
        };

        let results = ready_entries
            .par_iter()
            .map(|item| {
                let iops = item
                    .result
                    .jobs
                    .iter()
                    .map(|x| x.read.iops_mean + x.write.iops_mean)
                    .sum::<f64>();
                let bytes = item
                    .result
                    .jobs
                    .iter()
                    .map(|x| x.read.io_kbytes + x.write.io_kbytes)
                    .sum::<i64>();
                let latency = item.result.jobs.iter().map(mean_latency).sum::<f64>()
                    / item.result.jobs.len() as f64;
                let p99_latency = item.result.jobs.iter().map(mean_p99_latency).sum::<f64>();
                let x = *order.get(&self.get_order_key(item).unwrap()).unwrap();
                let y = if item.info.power_state == -1 {
                    0
                } else {
                    item.info.power_state
                } as usize;

                (
                    x,
                    y,
                    (iops / 1000.0) / item.ssd_power,
                    (iops / 1000.0) / (item.cpu_power + item.ssd_power),
                    (bytes as f64 / 1024.0) / item.ssd_power,
                    (bytes as f64 / 1024.0) / (item.cpu_power + item.ssd_power),
                    item.ssd_power * latency.powi(2),
                    item.ssd_power * p99_latency.powi(2),
                )
            })
            .collect::<Vec<_>>();
        for item in results {
            let x = item.0;
            let y = item.1;
            iops_j[x][y] = item.2;
            iops_j_cpu[x][y] = item.3;
            bytes_j[x][y] = item.4;
            bytes_j_cpu[x][y] = item.5;
            edp[x][y] = item.6;
            edp_p99[x][y] = item.7;
        }

        let plot_data_dir = plot_path.join("plot_data");
        if !plot_data_dir.exists() {
            create_dir_all(&plot_data_dir).await?;
        }

        fn write_json<T: Serialize>(
            data: &T,
            path: &Path,
            plot_data_dir: &Path,
        ) -> Result<PathBuf> {
            let p = plot_data_dir.join(format!(
                "{}.json",
                path.file_stem().unwrap().to_str().unwrap()
            ));
            write(&p, &serde_json::to_string(data)?)?;
            Ok(p)
        }

        let jobs = [
            (
                plot_path.join(format!("{}-iops-j.pdf", &experiment_name)),
                iops_j,
                "KIOPS/J",
                false,
            ),
            (
                plot_path.join(format!("{}-+cpu-iops-j.pdf", &experiment_name)),
                iops_j_cpu,
                "KIOPS/J",
                false,
            ),
            (
                plot_path.join(format!("{}-bytes-j.pdf", &experiment_name)),
                bytes_j,
                "MiB/J",
                false,
            ),
            (
                plot_path.join(format!("{}-+cpu-bytes-j.pdf", &experiment_name)),
                bytes_j_cpu,
                "MiB/J",
                false,
            ),
            (
                plot_path.join(format!("{}-edp.pdf", &experiment_name)),
                edp,
                "EDP",
                true,
            ),
            (
                plot_path.join(format!("{}-edp-p99.pdf", &experiment_name)),
                edp_p99,
                "P99 EDP",
                true,
            ),
        ];

        let results = jobs
            .par_iter()
            .map(|(filepath, data, title, reverse)| {
                let data_file = write_json(&data, filepath, &plot_data_dir).unwrap();
                plot_python(
                    "efficiency",
                    &[
                        ("--data", data_file.to_str().unwrap()),
                        ("--filepath", filepath.to_str().unwrap()),
                        ("--col_labels", &labels.join(",")),
                        ("--x_label", &self.x_label),
                        ("--experiment_name", &experiment_name),
                        ("--title", title),
                        ("--reverse", if *reverse { "1" } else { "0" }),
                    ],
                )
            })
            .collect::<Vec<_>>();
        for item in results {
            item?;
        }
        Ok(())
    }

    fn bar_plot(
        &self,
        ready_entries: Vec<PlotEntry>,
        settings: &Settings,
        filepath: PathBuf,
        plotting_file: &str,
        name: Option<&str>,
        get_mean: fn(&PlotEntry) -> f64,
    ) -> Result<()> {
        let num_power_states = settings.nvme_power_states.clone().unwrap_or(vec![0]).len();
        let mut results = vec![vec![]; num_power_states];
        let (order, labels) = self.get_order_labels(&ready_entries)?;

        let experiment_name = match &self.group {
            Some(group) => group.name.clone(),
            None => ready_entries[0].info.name.clone(),
        };

        for item in ready_entries {
            let mean = get_mean(&item);
            let ps = if item.info.power_state == -1 {
                0
            } else {
                item.info.power_state
            };
            results[ps as usize].push((item, mean));
        }

        for item in results.iter_mut() {
            item.sort_by_key(|entry| order.get(&self.get_order_key(&entry.0).unwrap()).unwrap());
        }

        let plot_data_dir = filepath.parent().unwrap().join("plot_data");
        if !plot_data_dir.exists() {
            std::fs::create_dir_all(&plot_data_dir)?;
        }

        let results = results
            .iter()
            .map(|x| x.iter().map(|x| x.1).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let plot_data_file = plot_data_dir.join(format!(
            "{}.json",
            &filepath.file_stem().unwrap().to_str().unwrap()
        ));
        write(&plot_data_file, &serde_json::to_string(&results)?)?;
        let labels = labels.join(",");
        let mut args = vec![
            ("--data", plot_data_file.to_str().unwrap()),
            ("--filepath", filepath.to_str().unwrap()),
            ("--x_label_name", &self.x_label),
            ("--experiment_name", &experiment_name),
            ("--labels", &labels),
        ];

        if let Some(name) = name {
            args.push(("--name", name));
        }

        plot_python(plotting_file, &args)?;
        Ok(())
    }

    fn get_order_key(&self, entry: &PlotEntry) -> Result<String> {
        Ok(match self.variable.as_str() {
            "request_sizes" => entry.args.request_sizes[0].clone(),
            "io_depths" => entry.args.io_depths[0].to_string(),
            "num_jobs" => entry.args.num_jobs.as_ref().unwrap()[0].to_string(),
            "extra_options" => {
                if entry.plot.group.is_some() {
                    entry.plot.group.as_ref().unwrap().x_label.clone()
                } else if entry.plot.labels.is_some() {
                    entry.args.extra_options.as_ref().unwrap()[0]
                        .clone()
                        .join(" ")
                } else {
                    bail!("extra_options expects either group or labels")
                }
            }
            _ => bail!("Unsupported plot variable {}", self.variable),
        })
    }

    fn get_order_labels(
        &self,
        ready_entries: &[PlotEntry],
    ) -> Result<(HashMap<String, usize>, Vec<String>)> {
        match self.variable.as_str() {
            "request_sizes" => {
                let set = ready_entries
                    .iter()
                    .map(|x| {
                        (
                            parse_data_size(&x.args.request_sizes[0]).unwrap(),
                            x.args.request_sizes[0].clone(),
                        )
                    })
                    .collect::<HashSet<_>>();
                let data = set.iter().sorted_by(|a, b| a.0.cmp(&b.0));

                Ok((
                    data.clone()
                        .enumerate()
                        .map(|(x, y)| (y.1.clone(), x))
                        .collect(),
                    data.map(|x| x.1.clone()).collect(),
                ))
            }
            "num_jobs" | "io_depths" => {
                let set = ready_entries
                    .iter()
                    .map(|item| match self.variable.as_str() {
                        "num_jobs" => item.args.num_jobs.as_ref().unwrap()[0],
                        "io_depths" => item.args.io_depths[0],
                        _ => unreachable!(),
                    })
                    .collect::<HashSet<_>>();
                let data = set.into_iter().sorted();
                Ok((
                    data.clone()
                        .enumerate()
                        .map(|(x, y)| (y.to_string(), x))
                        .collect(),
                    data.map(|x| x.to_string()).collect(),
                ))
            }
            "extra_options" => {
                if ready_entries.iter().any(|x| x.plot.group.is_some()) {
                    let data = ready_entries
                        .iter()
                        .map(|x| x.plot.group.as_ref().unwrap().x_label.clone())
                        .collect::<HashSet<_>>();
                    let data = data.into_iter().sorted();
                    Ok((
                        data.clone()
                            .enumerate()
                            .map(|(x, y)| (y.clone(), x))
                            .collect(),
                        data.collect(),
                    ))
                } else if ready_entries.iter().any(|x| x.plot.labels.is_some()) {
                    let data = ready_entries
                        .iter()
                        .map(|x| x.args.extra_options.as_ref().unwrap()[0].clone())
                        .collect::<HashSet<_>>();
                    let data = data.iter().sorted();
                    let labels = ready_entries[0]
                        .plot
                        .labels
                        .as_ref()
                        .unwrap()
                        .clone()
                        .into_iter()
                        .sorted();
                    Ok((
                        data.into_iter()
                            .enumerate()
                            .map(|(x, y)| (y.join(" "), x))
                            .collect(),
                        labels.collect(),
                    ))
                } else {
                    bail!("extra_options expects either group or labels")
                }
            }
            _ => bail!("Unsupported variable {}", self.variable),
        }
    }
}

fn mean_latency(x: &Job) -> f64 {
    let n = x.read.clat_ns.mean + x.write.clat_ns.mean;
    let mut d = 0;
    if x.read.clat_ns.mean > 0.0 {
        d += 1000000;
    }
    if x.write.clat_ns.mean > 0.0 {
        d += 1000000;
    }
    n / d as f64
}

fn mean_p99_latency(x: &Job) -> f64 {
    let r = x
        .read
        .clat_ns
        .percentile
        .as_ref()
        .map(|x| x.n99_000000)
        .unwrap_or(0);
    let w = x
        .write
        .clat_ns
        .percentile
        .as_ref()
        .map(|x| x.n99_000000)
        .unwrap_or(0);
    let mut d = 0;
    if r > 0 {
        d += 1000000;
    }
    if w > 0 {
        d += 1000000;
    }
    (r + w) as f64 / d as f64
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FioBwOverTime {
    variable: String,
}

#[async_trait::async_trait]
#[typetag::serde]
impl Plot for FioBwOverTime {
    fn required_sensors(&self) -> &'static [&'static str] {
        &["Powersensor3", "Rapl", "Sysinfo"]
    }

    async fn plot(
        &self,
        plot_type: &PlotType,
        data_path: &Path,
        plot_path: &Path,
        config_yaml: &Config,
        info: &HashMap<String, BenchmarkInfo>,
        dirs: Vec<String>,
        _: &Settings,
        _: &mut Vec<String>,
    ) -> Result<()> {
        if *plot_type == PlotType::Total {
            return Ok(());
        }

        let mut groups = HashMap::new();
        for run in dirs {
            let item = info.get(&run).context("No info for run")?;
            let key = (item.name.clone(), item.power_state, item.hash.clone());
            groups.entry(key).or_insert_with(|| (run, item.clone()));
        }

        let entries = groups.drain().map(|(_, x)| x).collect::<Vec<_>>();

        let bw_dir = plot_path.join("fio_time");
        let bw_inner_dir = bw_dir.join(&entries[0].1.name);
        create_dir_all(&bw_inner_dir).await?;
        entries.par_iter().for_each(|data| {
            let config_yaml = config_yaml.benches.iter().find(|x| x.name.eq(&data.1.name));
            let config_yaml = config_yaml
                .as_ref()
                .unwrap()
                .bench
                .downcast_ref::<Fio>()
                .unwrap();
            self.fio_time(
                data_path.join(data.0.clone()),
                &bw_inner_dir,
                config_yaml,
                &data.1,
            )
            .expect("Error running fio_time");
        });
        Ok(())
    }
}

impl FioBwOverTime {
    fn fio_time(
        &self,
        data_path: PathBuf,
        plot_path: &Path,
        _config_yaml: &Fio,
        info: &BenchmarkInfo,
    ) -> Result<()> {
        let config = info.args.downcast_ref::<Fio>().unwrap();
        let variable = match self.variable.as_str() {
            "request_sizes" => config.request_sizes[0].clone(),
            "io_depths" => config.io_depths[0].to_string(),
            "num_jobs" => config.num_jobs.as_ref().unwrap()[0].to_string(),
            "extra_options" => config.extra_options.as_ref().unwrap()[0].join("-"),
            _ => bail!("Unsupported plot variable {}", self.variable),
        };
        let name = format!("{}-{}-{}", info.name, info.power_state, variable);
        let mut child = std::process::Command::new("python3")
            .args([
                "plots/fio_time.py",
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
}
