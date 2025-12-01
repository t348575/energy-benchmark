use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
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
    pub matched_labels: Option<Vec<MatchedLabelEntry>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MatchedLabelEntry {
    pub label: String,
    pub items: Vec<usize>,
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
            .into_iter()
            .map(|item| {
                let (json, powersensor3, rapl, sysinfo, system, dir, info, plot) = item;
                let rapl = rapl.context("Read rapl").unwrap();
                let powersensor3 = powersensor3.context("Read powersensor3").unwrap();
                let sysinfo = sysinfo.context("Read sysinfo").unwrap();
                let system = system.context("Read system power").unwrap();
                let fio_result = json
                    .context(format!(
                        "Could not parse fio results.json in {dir} for {info:#?}"
                    ))
                    .unwrap();

                let parse_ramp_time = |ramp_time: &Option<String>| match ramp_time {
                    Some(x) => parse_time(&x).context("Parse ramp time").unwrap(),
                    None => 0,
                };
                let ramp_time = if let Some(g) = &fio_result.global_options {
                    parse_ramp_time(&g.ramp_time)
                } else {
                    parse_ramp_time(&fio_result.jobs[0].job_options.ramp_time)
                };

                let markers = format!("time,marker_name\n{ramp_time},ramp_time\n");

                let (rapl, _, _) = calculate_sectioned::<_, 2>(
                    Some(&markers),
                    &rapl,
                    &["Total"],
                    &[(0.0, settings.cpu_max_power_watts)],
                    power_energy_calculator,
                )
                .context("Calculate rapl means")
                .unwrap();

                let (ps3, _, _) = calculate_sectioned::<_, 2>(
                    Some(&markers),
                    &powersensor3,
                    &["Total"],
                    &[(0.0, bench_info.device_power_states[0].0)],
                    power_energy_calculator,
                )
                .context("Calculate powersensor3 means")
                .unwrap();

                let (sysinfo, _, _) = calculate_sectioned::<_, 2>(
                    Some(&markers),
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
                )
                .context("Calculate sysinfo means")
                .unwrap();

                let (system, _, _) = calculate_sectioned::<_, 2>(
                    Some(&markers),
                    &system,
                    &[r#"load-\S+"#],
                    &[(0.0, settings.cpu_max_power_watts * 2.0)],
                    power_energy_calculator,
                )
                .context("Calculate system power means")
                .unwrap();

                PlotEntry {
                    result: fio_result,
                    args: info.args.downcast_ref::<Fio>().unwrap().clone(),
                    info,
                    ssd_power: ps3[1],
                    cpu_power: rapl[1],
                    system_power: system[1],
                    plot: plot.clone(),
                    freq: sysinfo[1].0,
                    load: sysinfo[1].1,
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
                    (((data
                        .result
                        .jobs
                        .iter()
                        .map(|x| x.read.io_bytes)
                        .sum::<i64>()
                        + data
                            .result
                            .jobs
                            .iter()
                            .map(|x| x.write.io_bytes)
                            .sum::<i64>()) as f64)
                        / 1048576.0)
                        / (data.result.jobs[0].job_runtime as f64 / 1000.0)
                },
            ),
            (
                ready_entries.clone(),
                settings,
                throughput_dir.join(format!("{experiment_name}-iops.pdf")),
                BarChartKind::Throughput,
                None,
                |data| {
                    data.result
                        .jobs
                        .iter()
                        .map(|x| x.read.iops_mean + x.write.iops_mean)
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
                |data| data.ssd_power.power_mean.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-cpu.pdf")),
                BarChartKind::Power,
                Some("CPU"),
                |data| data.cpu_power.power_mean.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-norm-cpu.pdf")),
                BarChartKind::NormalizedPower,
                Some("CPU"),
                |data| data.cpu_power.power_mean.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-stdev-cpu.pdf")),
                BarChartKind::Power,
                Some("CPU"),
                |data| data.cpu_power.power_stddev.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-rolling-stdev-cpu.pdf")),
                BarChartKind::Power,
                Some("CPU"),
                |data| data.cpu_power.power_stddev_rolling_100ms.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-system.pdf")),
                BarChartKind::Power,
                Some("System"),
                |data| data.system_power.power_mean.unwrap(),
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
            .map(|x| self.bar_plot(x.0, x.1, x.2, x.3, x.4, x.5, bench_info, config_yaml))
            .collect::<Vec<_>>();
        for item in results {
            item?;
        }

        let efficiency_dir = plot_path.join("efficiency");
        self.efficiency(
            ready_entries.clone(),
            settings,
            &efficiency_dir,
            config_yaml,
        )
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
        config: &Config,
    ) -> Result<()> {
        let num_power_states = settings.nvme_power_states.clone().unwrap_or(vec![0]).len();
        let (order, labels) = self.get_order_labels(config, &ready_entries);
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
                let mb_s = (((item
                    .result
                    .jobs
                    .iter()
                    .map(|x| x.read.io_bytes)
                    .sum::<i64>()
                    + item
                        .result
                        .jobs
                        .iter()
                        .map(|x| x.write.io_bytes)
                        .sum::<i64>()) as f64)
                    / 1048576.0)
                    / (item.result.jobs[0].job_runtime as f64 / 1000.0);
                let latency = item.result.jobs.iter().map(mean_latency).sum::<f64>()
                    / item.result.jobs.len() as f64;
                let p99_latency = item.result.jobs.iter().map(mean_p99_latency).sum::<f64>();
                let x = *order
                    .get(&self.get_order_key(item.clone(), config))
                    .unwrap();
                let y = if item.info.power_state == -1 {
                    0
                } else {
                    item.info.power_state
                } as usize;

                (
                    x,
                    y,
                    (iops) / item.ssd_power.power_mean.unwrap(),
                    (iops)
                        / (item.cpu_power.power_mean.unwrap() + item.ssd_power.power_mean.unwrap()),
                    mb_s / item.ssd_power.power_mean.unwrap(),
                    mb_s / (item.cpu_power.power_mean.unwrap()
                        + item.ssd_power.power_mean.unwrap()),
                    item.ssd_power.power_mean.unwrap() * latency.powi(2),
                    item.ssd_power.power_mean.unwrap() * p99_latency.powi(2),
                    (item.ssd_power.power_mean.unwrap() + item.cpu_power.power_mean.unwrap())
                        * latency.powi(2),
                    mb_s / item.cpu_power.power_mean.unwrap(),
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
        config: &Config,
    ) -> Result<()> {
        let num_power_states = settings.nvme_power_states.clone().unwrap_or(vec![0]).len();
        let mut results = vec![vec![]; num_power_states];
        let (order, labels) = self.get_order_labels(config, &ready_entries);

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
            item.sort_by_key(|entry| {
                order
                    .get(&self.get_order_key(entry.0.clone(), config))
                    .unwrap()
            });
        }

        match chart_kind {
            BarChartKind::NormalizedPower => {
                for item in &mut results {
                    let base = item[0].1;
                    for (_, v) in item.iter_mut() {
                        *v /= base;
                        *v -= 1.0;
                        *v *= 100.0;
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

    fn get_variable_ordering<'a>(
        &self,
        config: &Config,
        variable: &str,
        ready_entries: &'a [PlotEntry],
    ) -> Vec<(&'a PlotEntry, String, String, usize)> {
        struct OrderingEntry<'a, T: Eq + Hash + Ord + ToString> {
            entry: &'a PlotEntry,
            value: T,
            label: String,
        }

        impl<T: Eq + Hash + Ord + ToString> Hash for OrderingEntry<'_, T> {
            fn hash<H: Hasher>(&self, state: &mut H) {
                self.value.hash(state);
            }
        }

        impl<T: Eq + Hash + Ord + ToString> PartialEq for OrderingEntry<'_, T> {
            fn eq(&self, other: &Self) -> bool {
                self.value == other.value
            }
        }
        impl<T: Eq + Hash + Ord + ToString> Eq for OrderingEntry<'_, T> {}

        fn finalize_hashset<T: Eq + Hash + Ord + ToString>(
            set: HashSet<OrderingEntry<'_, T>>,
            use_label_as_value: bool,
        ) -> Vec<(&'_ PlotEntry, String, String, usize)> {
            let data = set.into_iter().sorted_by(|a, b| a.value.cmp(&b.value));
            data.enumerate()
                .map(|(x, y)| {
                    (
                        y.entry,
                        if use_label_as_value {
                            y.label.clone()
                        } else {
                            y.value.to_string()
                        },
                        y.label,
                        x,
                    )
                })
                .collect()
        }

        match variable {
            "request_sizes" => {
                let set = ready_entries
                    .iter()
                    .map(|x| OrderingEntry {
                        entry: x,
                        value: parse_data_size(&x.args.request_sizes[0]).unwrap(),
                        label: x.args.request_sizes[0].clone(),
                    })
                    .collect::<HashSet<_>>();
                finalize_hashset(set, true)
            }
            "io_engines" => {
                if ready_entries
                    .iter()
                    .any(|x| x.plot.matched_labels.is_some())
                {
                    let data = ready_entries
                        .iter()
                        .map(|x| {
                            let key = x.args.matched_args.as_ref().unwrap();
                            let config_args = config
                                .benches
                                .iter()
                                .find(|b| b.name.eq(&x.info.name))
                                .unwrap();
                            let fio_opts = config_args.bench.downcast_ref::<Fio>().unwrap();

                            let matched_indexes = fio_opts
                                .matched_args
                                .as_ref()
                                .unwrap()
                                .iter()
                                .enumerate()
                                .filter_map(|(idx, x)| {
                                    if key.iter().find(|y| y.eq(&x)).is_some() {
                                        Some(idx)
                                    } else {
                                        None
                                    }
                                })
                                .collect::<Vec<_>>();
                            for label in x.plot.matched_labels.as_ref().unwrap() {
                                if !label.items.is_empty()
                                    && label.items.iter().all(|x| matched_indexes.contains(x))
                                {
                                    let key = key
                                        .iter()
                                        .map(|x| format!("{}={}", x.key, x.value.join(", ")))
                                        .join(" ");

                                    return OrderingEntry {
                                        entry: x,
                                        value: format!("{}-{key}", x.args.io_engines[0]),
                                        label: label.label.clone(),
                                    };
                                } else if matched_indexes.is_empty() && label.items.is_empty() {
                                    return OrderingEntry {
                                        entry: x,
                                        value: x.args.io_engines[0].clone(),
                                        label: label.label.clone(),
                                    };
                                }
                            }
                            unreachable!(
                                "Could not find matched label for args: {}",
                                key.iter()
                                    .map(|x| format!("{}={}", x.key, x.value.join(", ")))
                                    .join(" ")
                            )
                        })
                        .collect::<HashSet<_>>();

                    finalize_hashset(data, false)
                } else {
                    let set = ready_entries
                        .iter()
                        .map(|x| OrderingEntry {
                            entry: x,
                            value: x.args.io_engines[0].clone(),
                            label: x.args.io_engines[0].clone(),
                        })
                        .collect::<HashSet<_>>();

                    finalize_hashset(set, false)
                }
            }
            "num_jobs" | "io_depths" => {
                let set = ready_entries
                    .iter()
                    .map(|item| {
                        let x = match variable {
                            "num_jobs" => item.args.num_jobs.as_ref().unwrap()[0],
                            "io_depths" => item.args.io_depths[0],
                            _ => unreachable!(),
                        };
                        OrderingEntry {
                            entry: item,
                            value: x,
                            label: x.to_string(),
                        }
                    })
                    .collect::<HashSet<_>>();
                finalize_hashset(set, true)
            }
            "extra_options" => {
                if ready_entries.iter().any(|x| x.plot.group.is_some()) {
                    let data = ready_entries
                        .iter()
                        .map(|x| OrderingEntry {
                            entry: x,
                            value: x.plot.group.as_ref().unwrap().x_label.clone(),
                            label: x.plot.group.as_ref().unwrap().x_label.clone(),
                        })
                        .collect::<HashSet<_>>();
                    finalize_hashset(data, false)
                } else if ready_entries.iter().any(|x| x.plot.labels.is_some()) {
                    let data = ready_entries
                        .iter()
                        .map(|x| {
                            let key = x.args.extra_options.as_ref().unwrap()[0].clone();
                            let config_args = config
                                .benches
                                .iter()
                                .find(|b| b.name.eq(&x.info.name))
                                .unwrap();
                            let fio_opts = config_args.bench.downcast_ref::<Fio>().unwrap();
                            let pos = fio_opts
                                .extra_options
                                .as_ref()
                                .unwrap()
                                .iter()
                                .position(|f| f.join(" ").eq(&key.join(" ")))
                                .unwrap();
                            OrderingEntry {
                                entry: x,
                                value: key.join(" "),
                                label: x.plot.labels.as_ref().unwrap()[pos].clone(),
                            }
                        })
                        .collect::<HashSet<_>>();
                    finalize_hashset(data, false)
                } else {
                    unreachable!("extra_options expects either group or labels")
                }
            }
            _ => unreachable!("Unsupported variable {}", variable),
        }
    }

    fn get_order_key(&self, entry: PlotEntry, config: &Config) -> String {
        self.get_order_labels(&config, &[entry.clone()])
            .0
            .iter()
            .next()
            .unwrap()
            .0
            .clone()
    }

    fn get_order_labels(
        &self,
        config: &Config,
        ready_entries: &[PlotEntry],
    ) -> (HashMap<String, usize>, Vec<String>) {
        let items = self
            .variables
            .iter()
            .map(|var| self.get_variable_ordering(config, var, ready_entries))
            .collect::<Vec<_>>();
        let entries = items.iter().map(|x| x.iter()).multi_cartesian_product();

        let mut order = HashMap::new();
        let mut labels = Vec::new();
        for (idx, entry) in entries.enumerate() {
            let entry_str = entry.iter().map(|x| &x.1).join("-");
            let label = entry.iter().map(|x| &x.2).join(" ");
            order.insert(entry_str.clone(), idx);
            labels.push(label);
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
#[serde(deny_unknown_fields)]
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
        let plot_data = bw_inner_dir.join("plot_data");
        ensure_dirs(&[bw_dir.clone(), bw_inner_dir.clone(), plot_data]).await?;

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
            "io_engines" => config.io_engines[0].to_owned(),
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

        let trace_file = data_path.join(group_dir).join("trace.out");
        if trace_file.exists() {
            let trace = common::util::parse_trace(
                &std::fs::File::open(&trace_file)?,
                &config.fs.clone().unwrap(),
            )?;
            common::util::write_csv(
                &plot_path.join("plot_data").join(format!("{name}.csv")),
                &trace,
            )?;
        }

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
