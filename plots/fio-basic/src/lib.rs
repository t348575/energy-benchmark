use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use common::{
    bench::{BenchInfo, BenchParams},
    config::{Config, Settings},
    plot::{HeatmapJob, Plot, PlotType, collect_run_groups, ensure_dirs, render_heatmaps},
    util::{
        BarChartKind, SectionStats, TimeSeriesAxis, TimeSeriesPlot, TimeSeriesSpec,
        calculate_sectioned, make_power_state_bar_config, parse_data_size, parse_time,
        plot_bar_chart, plot_time_series, power_energy_calculator, read_json_file,
        sysinfo_average_calculator,
    },
};
use default_benches::BenchKind;
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
use tokio::fs::read_to_string;
use tracing::debug;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FioBasic {
    pub variables: Vec<String>,
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
    info: BenchParams,
    args: Fio,
    ssd_power: SectionStats,
    cpu_power: SectionStats,
    system_power: SectionStats,
    plot: FioBasic,
    load: f64,
    freq: f64,
}

#[async_trait::async_trait]
#[typetag::serde]
impl Plot for FioBasic {
    fn required_sensors(&self) -> &'static [&'static str] {
        &["Powersensor3", "Rapl", "Sysinfo", "Diskstat"]
    }

    async fn plot(
        &self,
        plot_type: &PlotType,
        data_path: &Path,
        plot_path: &Path,
        config_yaml: &Config,
        bench_info: &BenchInfo,
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
                        r.is_match(&bench_info.param_map.get(x).unwrap().name)
                            && !completed_dirs.contains(x)
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
        let groups = collect_run_groups(dirs, &bench_info.param_map, completed_dirs)?;
        if groups.is_empty() {
            return Ok(());
        }

        let groups = groups
            .into_iter()
            .map(|group| {
                let plots_cfg = config_yaml
                    .benches
                    .iter()
                    .find(|x| x.name.eq(&group.info.name))
                    .context("No config for run")?
                    .plots
                    .as_ref()
                    .context("No plots for run")?;
                let plot_obj = plots_cfg
                    .iter()
                    .find(|x| x.is::<FioBasic>())
                    .unwrap()
                    .downcast_ref::<FioBasic>()
                    .unwrap()
                    .clone();
                Ok((group, plot_obj))
            })
            .collect::<Result<Vec<_>>>()?;

        let entries = join_all(groups.iter().map(|(group, plot)| {
            let run_dir = data_path.join(&group.dir);
            let plot_clone = plot.clone();
            let info_clone = group.info.clone();
            async move {
                let results = read_json_file::<FioResult>(run_dir.join("results.json")).await;
                let ps3 = read_to_string(run_dir.join("powersensor3.csv")).await;
                let rapl = read_to_string(run_dir.join("rapl.csv")).await;
                let sysinfo = read_to_string(run_dir.join("sysinfo.csv")).await;
                let system = read_to_string(run_dir.join("netio-http.csv")).await;
                (
                    results,
                    ps3,
                    rapl,
                    sysinfo,
                    system,
                    group.dir.clone(),
                    info_clone,
                    plot_clone,
                )
            }
        }))
        .await;

        let ready_entries = entries
            .into_par_iter()
            .map(|item| {
                let (json, powersensor3, rapl, sysinfo, system, _, info, plot) = item;
                let rapl = rapl.context("Read rapl").unwrap();
                let powersensor3 = powersensor3.context("Read powersensor3").unwrap();
                let sysinfo = sysinfo.context("Read sysinfo").unwrap();
                let system = system.context("Read system power").unwrap();
                let fio_result = json
                    .context(format!("Could not parse fio results.json for {info:#?}"))
                    .unwrap();

                let runtime = fio_result
                    .jobs
                    .iter()
                    .max_by_key(|x| x.job_runtime)
                    .unwrap()
                    .job_runtime as usize;
                let ramp_time = match &fio_result.jobs[0].job_options.ramp_time {
                    Some(x) => parse_time(x).context("Parse ramp time").unwrap(),
                    None => 0,
                };

                let (_, rapl, _) = calculate_sectioned::<_, 0>(
                    None,
                    &rapl,
                    &["Total"],
                    &[(0.0, settings.cpu_max_power_watts)],
                    power_energy_calculator,
                    Some(runtime + ramp_time),
                )
                .context("Calculate rapl means")
                .unwrap();
                let (_, ps3, _) = calculate_sectioned::<_, 0>(
                    None,
                    &powersensor3,
                    &["Total"],
                    &[(0.0, bench_info.device_power_states[0].0)],
                    power_energy_calculator,
                    Some(runtime + ramp_time),
                )
                .context("Calculate powersensor3 means")
                .unwrap();

                let (_, (freq, load), _) = calculate_sectioned::<_, 0>(
                    None,
                    &sysinfo,
                    &["cpu-[0-9]{0,3}-freq", "cpu-[0-9]{0,3}-load"],
                    &[
                        (
                            bench_info.cpu_freq_limits.0 as f64 / 1000.0,
                            bench_info.cpu_freq_limits.1 as f64 / 1000.0,
                        ),
                        (0.0, f64::MAX),
                    ],
                    sysinfo_average_calculator,
                    Some(runtime + ramp_time),
                )
                .context("Calculate sysinfo means")
                .unwrap();

                let (_, system, _) = calculate_sectioned::<_, 0>(
                    None,
                    &system,
                    &[r#"load-\S+"#],
                    &[(0.0, settings.cpu_max_power_watts * 2.0)],
                    power_energy_calculator,
                    Some(runtime + ramp_time),
                )
                .context("Calculate system power means")
                .unwrap();

                PlotEntry {
                    result: fio_result,
                    args: info.args.downcast_ref::<Fio>().unwrap().clone(),
                    info,
                    ssd_power: ps3,
                    cpu_power: rapl,
                    system_power: system,
                    plot: plot.clone(),
                    freq,
                    load,
                }
            })
            .collect::<Vec<_>>();

        let experiment_name = match &self.group {
            Some(group) => group.name.clone(),
            None => ready_entries[0].info.name.clone(),
        };

        let throughput_dir = plot_path.join("throughput");
        let latency_dir = plot_path.join("latency");
        let power_dir = plot_path.join("power");
        let efficiency_dir = plot_path.join("efficiency");
        ensure_dirs(&[
            throughput_dir.clone(),
            latency_dir.clone(),
            power_dir.clone(),
            efficiency_dir.clone(),
        ])
        .await?;

        let plot_jobs: Vec<(
            Vec<PlotEntry>,
            &Settings,
            PathBuf,
            BarChartKind,
            Option<&str>,
            fn(&PlotEntry) -> f64,
        )> = vec![
            (
                ready_entries.clone(),
                settings,
                throughput_dir.join(format!("{experiment_name}.pdf")),
                BarChartKind::Throughput,
                None,
                |data| {
                    data.result
                        .jobs
                        .iter()
                        .map(|x| (x.read.bw_mean + x.write.bw_mean) / 1024.0)
                        .sum::<f64>()
                },
            ),
            (
                ready_entries.clone(),
                settings,
                latency_dir.join(format!("{experiment_name}.pdf")),
                BarChartKind::Latency,
                None,
                |data| data.result.jobs.iter().map(mean_latency).sum::<f64>(),
            ),
            (
                ready_entries.clone(),
                settings,
                latency_dir.join(format!("{experiment_name}-p99.pdf")),
                BarChartKind::Latency,
                None,
                |data| data.result.jobs.iter().map(mean_p99_latency).sum::<f64>(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-ssd.pdf")),
                BarChartKind::Power,
                Some("SSD"),
                |data| data.ssd_power.power.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-cpu.pdf")),
                BarChartKind::Power,
                Some("CPU"),
                |data| data.cpu_power.power.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-norm-cpu.pdf")),
                BarChartKind::NormalizedPower,
                Some("CPU"),
                |data| data.cpu_power.power.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-system.pdf")),
                BarChartKind::Power,
                Some("System"),
                |data| data.system_power.power.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-freq.pdf")),
                BarChartKind::Freq,
                Some("CPU"),
                |data| data.freq,
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-load.pdf")),
                BarChartKind::Load,
                Some("Linux"),
                |data| data.load,
            ),
        ];

        let results = plot_jobs
            .into_par_iter()
            .map(|x| self.bar_plot(x.0, x.1, x.2, x.3, x.4, x.5, bench_info))
            .collect::<Vec<_>>();
        for item in results {
            item?;
        }

        let efficiency_dir = plot_path.join("efficiency");
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
        let (order, labels) = self.get_order_labels(&ready_entries);
        let mut iops_j = vec![vec![0f64; num_power_states]; order.len()];
        let mut iops_j_cpu = iops_j.clone();
        let mut edp = iops_j.clone();
        let mut edp_p99 = iops_j.clone();
        let mut edp_total = iops_j.clone();
        let mut bytes_j = iops_j.clone();
        let mut cpu_only_bytes_j = iops_j.clone();
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
                    .map(|x| x.read.bw_mean + x.write.bw_mean)
                    .sum::<f64>();
                let latency = item.result.jobs.iter().map(mean_latency).sum::<f64>()
                    / item.result.jobs.len() as f64;
                let p99_latency = item.result.jobs.iter().map(mean_p99_latency).sum::<f64>();
                let x = *order.get(&self.get_order_key(item)).unwrap();
                let y = if item.info.power_state == -1 {
                    0
                } else {
                    item.info.power_state
                } as usize;

                (
                    x,
                    y,
                    (iops) / item.ssd_power.power.unwrap(),
                    (iops) / (item.cpu_power.power.unwrap() + item.ssd_power.power.unwrap()),
                    (bytes / 1024.0) / item.ssd_power.power.unwrap(),
                    (bytes / 1024.0)
                    / (item.cpu_power.power.unwrap() + item.ssd_power.power.unwrap()),
                    item.ssd_power.power.unwrap() * latency.powi(2),
                    item.ssd_power.power.unwrap() * p99_latency.powi(2),
                    (item.ssd_power.power.unwrap() + item.cpu_power.power.unwrap())
                    * latency.powi(2),
                    (bytes / 1024.0) / item.cpu_power.power.unwrap(),
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
            edp_total[x][y] = item.8;
            cpu_only_bytes_j[x][y] = item.9;
        }

        let x_label = self.x_label.as_str();
        let jobs = vec![
            HeatmapJob {
                filepath: plot_path.join(format!("{}-iops-j.pdf", &experiment_name)),
                data: iops_j,
                title: "IOPS/J",
                x_label,
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-+cpu-iops-j.pdf", &experiment_name)),
                data: iops_j_cpu,
                title: "IOPS/J",
                x_label,
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-bytes-j.pdf", &experiment_name)),
                data: bytes_j,
                title: "MiB/J",
                x_label,
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-only-cpu-bytes-j.pdf", &experiment_name)),
                data: cpu_only_bytes_j,
                title: "MiB/J",
                x_label,
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-+cpu-bytes-j.pdf", &experiment_name)),
                data: bytes_j_cpu,
                title: "MiB/J",
                x_label,
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-edp.pdf", &experiment_name)),
                data: edp,
                title: "EDP",
                x_label,
                reverse: true,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-edp-p99.pdf", &experiment_name)),
                data: edp_p99,
                title: "P99 EDP",
                x_label,
                reverse: true,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-edp-total.pdf", &experiment_name)),
                data: edp_total,
                title: "EDP total",
                x_label,
                reverse: true,
            },
        ];

        render_heatmaps(&experiment_name, &labels, plot_path, &jobs)
    }

    fn bar_plot(
        &self,
        ready_entries: Vec<PlotEntry>,
        settings: &Settings,
        filepath: PathBuf,
        chart_kind: BarChartKind,
        name: Option<&str>,
        get_mean: fn(&PlotEntry) -> f64,
        bench_info: &BenchInfo,
    ) -> Result<()> {
        let num_power_states = settings.nvme_power_states.clone().unwrap_or(vec![0]).len();
        let mut results = vec![vec![]; num_power_states];
        let (_, labels) = self.get_order_labels(&ready_entries);

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
            item.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        }

        match chart_kind {
            BarChartKind::NormalizedPower => {
                for item in &mut results {
                    let base = item[0].1;
                    let factor = 100.0 / base;
                    for (_, v) in item.iter_mut() {
                        *v *= factor;
                    }
                }
            }
            _ => {}
        }

        let results = results
            .iter()
            .map(|x| x.iter().map(|x| x.1).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let config = make_power_state_bar_config(chart_kind, &self.x_label, &experiment_name, name);
        plot_bar_chart(&filepath, results, labels, config, bench_info)
    }

    fn get_order_key(&self, entry: &PlotEntry) -> String {
        let matcher = |variable: &str| match variable {
            "request_sizes" => entry.args.request_sizes[0].clone(),
            "io_depths" => entry.args.io_depths[0].to_string(),
            "num_jobs" => entry.args.num_jobs.as_ref().unwrap()[0].to_string(),
            "io_engine" => entry.args.io_engines[0].clone(),
            "extra_options" => {
                if entry.plot.group.is_some() {
                    entry.plot.group.as_ref().unwrap().x_label.clone()
                } else if entry.plot.labels.is_some() {
                    entry.args.extra_options.as_ref().unwrap()[0]
                        .clone()
                        .join(" ")
                } else {
                    unreachable!("extra_options expects either group or labels")
                }
            }
            _ => unreachable!("Unsupported plot variable {}", variable),
        };
        self.variables.iter().map(|var| matcher(var)).join("-")
    }

    fn get_variable_ordering(
        &self,
        variable: &str,
        ready_entries: &[PlotEntry],
    ) -> Vec<(String, usize)> {
        match variable {
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
                data.clone()
                    .enumerate()
                    .map(|(x, y)| (y.1.clone(), x))
                    .collect()
            }
            "io_engine" => {
                let set = ready_entries
                    .iter()
                    .map(|x| x.args.io_engines[0].clone())
                    .collect::<HashSet<_>>();
                let data = set.iter().sorted();
                data.clone()
                    .enumerate()
                    .map(|(x, y)| (y.to_string(), x))
                    .collect()
            }
            "num_jobs" | "io_depths" => {
                let set = ready_entries
                    .iter()
                    .map(|item| match variable {
                        "num_jobs" => item.args.num_jobs.as_ref().unwrap()[0],
                        "io_depths" => item.args.io_depths[0],
                        _ => unreachable!(),
                    })
                    .collect::<HashSet<_>>();
                let data = set.into_iter().sorted();
                data.clone()
                    .enumerate()
                    .map(|(x, y)| (y.to_string(), x))
                    .collect()
            }
            "extra_options" => {
                if ready_entries.iter().any(|x| x.plot.group.is_some()) {
                    let data = ready_entries
                        .iter()
                        .map(|x| x.plot.group.as_ref().unwrap().x_label.clone())
                        .collect::<HashSet<_>>();
                    let data = data.into_iter().sorted();
                    data.clone()
                        .enumerate()
                        .map(|(x, y)| (y.clone(), x))
                        .collect()
                } else if ready_entries.iter().any(|x| x.plot.labels.is_some()) {
                    let data = ready_entries
                        .iter()
                        .map(|x| x.args.extra_options.as_ref().unwrap()[0].clone())
                        .collect::<HashSet<_>>();
                    let data = data.iter().sorted();
                    data.into_iter()
                        .enumerate()
                        .map(|(x, y)| (y.join(" "), x))
                        .collect()
                } else {
                    unreachable!("extra_options expects either group or labels")
                }
            }
            _ => unreachable!("Unsupported variable {}", variable),
        }
    }

    fn get_order_labels(
        &self,
        ready_entries: &[PlotEntry],
    ) -> (HashMap<String, usize>, Vec<String>) {
        let items = self
            .variables
            .iter()
            .map(|var| self.get_variable_ordering(var, ready_entries))
            .collect::<Vec<_>>();
        let entries = items.iter().map(|x| x.iter()).multi_cartesian_product();

        let mut order = HashMap::new();
        let mut labels = Vec::new();
        for (idx, entry) in entries.enumerate() {
            let entry_str = entry.iter().map(|x| &x.0).join("-");
            order.insert(entry_str.clone(), idx);
            match &self.labels {
                Some(label_list) => labels.push(label_list[idx].clone()),
                None => labels.push(entry_str),
            }
        }
        (order, labels)
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
        _: &Config,
        bench_info: &BenchInfo,
        dirs: Vec<String>,
        _: &Settings,
        completed_dirs: &mut Vec<String>,
    ) -> Result<()> {
        if *plot_type == PlotType::Total {
            return Ok(());
        }

        let groups = collect_run_groups(dirs, &bench_info.param_map, completed_dirs)?;
        if groups.is_empty() {
            return Ok(());
        }

        let bw_dir = plot_path.join("fio_time");
        let bw_inner_dir = bw_dir.join(&groups[0].info.name);
        ensure_dirs(&[
            bw_dir.clone(),
            bw_inner_dir.clone(),
            bw_inner_dir.join("plot_data"),
        ])
        .await?;

        groups.par_iter().for_each(|group| {
            self.fio_time(
                data_path,
                &group.dir,
                &bw_inner_dir,
                &group.info,
                bench_info,
            )
            .expect("Error running fio_time");
        });
        Ok(())
    }
}

impl FioBwOverTime {
    fn fio_time(
        &self,
        data_path: &Path,
        group_dir: &str,
        plot_path: &Path,
        info: &BenchParams,
        bench_info: &BenchInfo,
    ) -> Result<()> {
        let config = info.args.downcast_ref::<Fio>().unwrap();
        let variable = match self.variable.as_str() {
            "request_sizes" => config.request_sizes[0].clone(),
            "io_depths" => config.io_depths[0].to_string(),
            "num_jobs" => config.num_jobs.as_ref().unwrap()[0].to_string(),
            "extra_options" => config.extra_options.as_ref().unwrap()[0].join("-"),
            "io_engine" => config.io_engines[0].to_owned(),
            _ => bail!("Unsupported plot variable {}", self.variable),
        };
        let name = format!("{}-{}-{}", info.name, info.power_state, variable);

        let default = TimeSeriesPlot::new(
            None,
            format!("{name}-throughput-verify"),
            "Fio throughput vs Diskstat throughput",
            TimeSeriesAxis::bench_time(),
            vec![TimeSeriesAxis::bench(
                "smoothed",
                "Throughput",
                "Throughput (MiB/s)",
            )],
        );

        plot_time_series(TimeSeriesSpec::new(
            BenchKind::Fio.name(),
            plot_path.to_path_buf(),
            data_path.join(group_dir),
            &name,
            vec![
                TimeSeriesPlot::new(
                    None,
                    format!("{name}-throughput-verify"),
                    "Fio throughput vs Diskstat throughput",
                    TimeSeriesAxis::bench_time(),
                    vec![TimeSeriesAxis::bench(
                        "smoothed",
                        "fio",
                        "Fio Throughput (MiB/s)",
                    )],
                )
                .with_secondary(diskstat::DISKSTAT_PLOT_AXIS.to_vec()),
                default
                    .clone()
                    .with_title("Throughput vs SSD power")
                    .with_filename(format!("{name}-ssd"))
                    .with_secondary(powersensor3::POWERSENSOR_PLOT_AXIS.to_vec()),
                default
                    .clone()
                    .with_title("Throughput vs CPU power")
                    .with_filename(format!("{name}-cpu"))
                    .with_secondary(rapl::RAPL_PLOT_AXIS.to_vec()),
                default
                    .clone()
                    .with_title("Throughput vs CPU freq")
                    .with_filename(format!("{name}-cpu-freq"))
                    .with_secondary(sysinfo::sysinfo_freq_plot_axis(&bench_info.cpu_topology)),
                default
                    .clone()
                    .with_title("Throughput vs CPU load")
                    .with_filename(format!("{name}-cpu-load"))
                    .with_secondary(sysinfo::sysinfo_load_plot_axis(&bench_info.cpu_topology)),
            ],
        ))?;
        Ok(())
    }
}
