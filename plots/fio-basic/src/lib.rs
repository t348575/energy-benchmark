use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use common::{
    bench::BenchmarkInfo,
    config::{Config, Settings},
    plot::{Plot, PlotType},
    util::{get_mean_power, parse_request_size, plot_python},
};
use eyre::{Context, ContextCompat, Result, bail};
use fio::{Fio, result::FioResult};
use itertools::Itertools;
use pyo3::{IntoPyObject, types::PyAnyMethods};
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::fs::{create_dir_all, read_to_string};

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
        &["Powersensor3", "Pmt"]
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

        async fn read_pmt_rapl(folder: PathBuf) -> Result<String> {
            Ok(read_to_string(folder.join("pmt-RAPL.csv")).await?)
        }

        let entries = groups
            .drain()
            .map(|(_, (folder, info, plot))| {
                (
                    read_results_json(data_path.join(folder.clone())),
                    read_powersensor3(data_path.join(folder.clone())),
                    read_pmt_rapl(data_path.join(folder.clone())),
                    folder,
                    info,
                    plot,
                )
            })
            .collect::<Vec<_>>();

        let mut ready_entries = Vec::new();
        for item in entries {
            let (json, powersensor3, pmt_rapl, _, info, plot) = item;
            let pmt_rapl = pmt_rapl.await?;
            ready_entries.push(PlotEntry {
                result: json.await?,
                args: info.args.downcast_ref::<Fio>().unwrap().clone(),
                info,
                ssd_power: get_mean_power(&powersensor3.await?, "Total")?,
                cpu_power: get_mean_power(&pmt_rapl, "Total")?,
                dram_power: get_mean_power(&pmt_rapl, "dram-1")?,
                node_0_power: get_mean_power(&pmt_rapl, "package-1")?,
                node_1_power: get_mean_power(&pmt_rapl, "package-1")?,
                plot: plot.clone(),
            });
        }

        let experiment_name = match &self.group {
            Some(group) => group.name.clone(),
            None => ready_entries[0].info.name.clone(),
        };

        let throughput_dir = plot_path.join("throughput");
        create_dir_all(&throughput_dir).await?;
        self.bar_plot(
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
        )?;

        let latency_dir = plot_path.join("latency");
        create_dir_all(&latency_dir).await?;
        self.bar_plot(
            ready_entries.clone(),
            settings,
            latency_dir.join(format!("{}.pdf", experiment_name)),
            "latency",
            None,
            |data| {
                data.result
                    .jobs
                    .iter()
                    .map(|x| (x.read.lat_ns.mean + x.write.lat_ns.mean) / 1000000.0)
                    .sum::<f64>()
            },
        )?;

        let power_dir = plot_path.join("power");
        create_dir_all(&power_dir).await?;
        self.bar_plot(
            ready_entries.clone(),
            settings,
            power_dir.join(format!("{}-ssd.pdf", experiment_name)),
            "power",
            Some("SSD"),
            |data| data.ssd_power,
        )?;
        self.bar_plot(
            ready_entries.clone(),
            settings,
            power_dir.join(format!("{}-cpu.pdf", experiment_name)),
            "power",
            Some("CPU + DRAM"),
            |data| data.cpu_power,
        )?;
        self.bar_plot(
            ready_entries.clone(),
            settings,
            power_dir.join(format!("{}-dram.pdf", experiment_name)),
            "power",
            Some("DRAM 1"),
            |data| data.dram_power,
        )?;
        self.bar_plot(
            ready_entries.clone(),
            settings,
            power_dir.join(format!("{}-node-0.pdf", experiment_name)),
            "power",
            Some("Node 0"),
            |data| data.node_0_power,
        )?;
        self.bar_plot(
            ready_entries.clone(),
            settings,
            power_dir.join(format!("{}-node-1.pdf", experiment_name)),
            "power",
            Some("Node 1"),
            |data| data.node_1_power,
        )?;

        let efficiency_dir = plot_path.join("efficiency");
        create_dir_all(&efficiency_dir).await?;
        self.efficiency(ready_entries.clone(), settings, &efficiency_dir)?;

        Ok(())
    }
}

impl FioBasic {
    fn efficiency(
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
        let mut bytes_j = iops_j.clone();
        let experiment_name = match &self.group {
            Some(group) => group.name.clone(),
            None => ready_entries[0].info.name.clone(),
        };

        for item in ready_entries {
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
                .map(|x| x.read.bw_mean + x.write.bw_mean)
                .sum::<f64>();
            let latency = item
                .result
                .jobs
                .iter()
                .map(|x| (x.read.lat_ns.mean + x.write.lat_ns.mean) / 1000000.0)
                .sum::<f64>();
            let key = match self.variable.as_str() {
                "request_sizes" => item.args.request_sizes[0].clone(),
                "io_depths" => item.args.io_depths[0].to_string(),
                "num_jobs" => item.args.num_jobs.as_ref().unwrap()[0].to_string(),
                "extra_options" => {
                    if item.plot.group.is_some() {
                        item.plot.group.unwrap().x_label
                    } else if item.plot.labels.is_some() {
                        item.args.extra_options.as_ref().unwrap()[0]
                            .clone()
                            .join(" ")
                    } else {
                        panic!("extra_options expects either group or labels")
                    }
                }
                _ => bail!("Unsupported plot variable {}", self.variable),
            };
            let x = *order.get(&key).unwrap();
            let y = item.info.power_state as usize;
            iops_j[x][y] = (iops / 1000.0) / item.ssd_power;
            iops_j_cpu[x][y] = (iops / 1000.0) / (item.cpu_power + item.ssd_power);
            bytes_j[x][y] = (bytes / 1024.0) / item.ssd_power;
            edp[x][y] = item.ssd_power * latency.powi(2);
        }

        plot_python(
            |py, module| {
                let iops_j = iops_j.into_pyobject(py)?;
                let filepath = plot_path.join(format!("{}-iops-j.pdf", &experiment_name));
                module.call1((
                    filepath.to_str().unwrap(),
                    iops_j,
                    &labels,
                    &self.x_label,
                    &experiment_name,
                    "KIOPS/J",
                ))?;
                Ok(())
            },
            "efficiency",
        )?;

        plot_python(
            |py, module| {
                let iops_j_cpu = iops_j_cpu.into_pyobject(py)?;
                let filepath = plot_path.join(format!("{}-+cpu-iops-j.pdf", &experiment_name));
                module.call1((
                    filepath.to_str().unwrap(),
                    iops_j_cpu,
                    &labels,
                    &self.x_label,
                    &experiment_name,
                    "KIOPS/J",
                ))?;
                Ok(())
            },
            "efficiency",
        )?;

        plot_python(
            |py, module| {
                let bytes_j = bytes_j.into_pyobject(py)?;
                let filepath = plot_path.join(format!("{}-bytes-j.pdf", &experiment_name));
                module.call1((
                    filepath.to_str().unwrap(),
                    bytes_j,
                    &labels,
                    &self.x_label,
                    &experiment_name,
                    "MiB/J",
                ))?;
                Ok(())
            },
            "efficiency",
        )?;

        plot_python(
            |py, module| {
                let edp = edp.into_pyobject(py)?;
                let filepath = plot_path.join(format!("{}-edp.pdf", &experiment_name));
                module.call1((
                    filepath.to_str().unwrap(),
                    edp,
                    &labels,
                    &self.x_label,
                    &experiment_name,
                    "EDP",
                    true,
                ))?;
                Ok(())
            },
            "efficiency",
        )?;
        Ok(())
    }

    fn bar_plot(
        &self,
        ready_entries: Vec<PlotEntry>,
        settings: &Settings,
        filepath: PathBuf,
        plotting_file: &str,
        y_name: Option<&str>,
        get_mean: fn(&PlotEntry) -> f64,
    ) -> Result<()> {
        let num_power_states = settings.nvme_power_states.clone().unwrap_or(vec![0]).len();
        let mut results = vec![vec![]; num_power_states];
        let (_, labels) = self.get_order_labels(&ready_entries)?;

        let experiment_name = match &self.group {
            Some(group) => group.name.clone(),
            None => ready_entries[0].info.name.clone(),
        };

        for item in ready_entries {
            let mean = get_mean(&item);
            results[item.info.power_state as usize].push((item, mean));
        }

        for item in results.iter_mut().take(num_power_states) {
            item.sort_by(|a, b| match self.variable.as_str() {
                "request_sizes" => parse_request_size(&a.0.args.request_sizes[0])
                    .unwrap()
                    .cmp(&parse_request_size(&b.0.args.request_sizes[0]).unwrap()),
                "io_depths" => a.0.args.io_depths[0].cmp(&b.0.args.io_depths[0]),
                "num_jobs" => a.0.args.num_jobs.as_ref().unwrap()[0]
                    .cmp(&b.0.args.num_jobs.as_ref().unwrap()[0]),
                "extra_options" => {
                    if a.0.plot.group.is_some() {
                        a.0.plot
                            .group
                            .as_ref()
                            .unwrap()
                            .x_label
                            .cmp(&b.0.plot.group.as_ref().unwrap().x_label)
                    } else if a.0.plot.labels.is_some() {
                        a.0.args.extra_options.as_ref().unwrap()[0]
                            .cmp(&b.0.args.extra_options.as_ref().unwrap()[0])
                    } else {
                        panic!("extra_options expects either group or labels")
                    }
                }
                _ => panic!("Unsupported plot variable {}", self.variable),
            });
        }

        plot_python(
            move |py, module| {
                let results = results
                    .iter()
                    .map(|x| x.iter().map(|x| x.1).collect::<Vec<_>>())
                    .collect::<Vec<_>>();
                let data = results.into_pyobject(py)?;
                match y_name {
                    Some(y_name) => module.call1((
                        data,
                        filepath.to_str().unwrap(),
                        &self.x_label,
                        experiment_name,
                        y_name,
                        labels,
                    ))?,
                    None => module.call1((
                        data,
                        filepath.to_str().unwrap(),
                        &self.x_label,
                        experiment_name,
                        labels,
                    ))?,
                };
                Ok(())
            },
            plotting_file,
        )?;
        Ok(())
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
                            parse_request_size(&x.args.request_sizes[0]).unwrap(),
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
                    Ok((
                        data.clone()
                            .into_iter()
                            .enumerate()
                            .map(|(x, y)| (y.clone(), x))
                            .collect(),
                        data.into_iter().collect(),
                    ))
                } else if ready_entries.iter().any(|x| x.plot.labels.is_some()) {
                    let data = ready_entries
                        .iter()
                        .map(|x| x.args.extra_options.as_ref().unwrap()[0].clone())
                        .collect::<HashSet<_>>();
                    let labels = ready_entries[0].plot.labels.as_ref().unwrap().clone();
                    Ok((
                        data.into_iter()
                            .enumerate()
                            .map(|(x, y)| (y.join(" "), x))
                            .collect(),
                        labels,
                    ))
                } else {
                    bail!("extra_options expects either group or labels")
                }
            }
            _ => bail!("Unsupported variable {}", self.variable),
        }
    }
}
