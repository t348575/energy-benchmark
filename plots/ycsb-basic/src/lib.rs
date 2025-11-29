use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use common::{
    bench::{BenchInfo, BenchParams},
    config::{Config, Settings},
    plot::{HeatmapJob, Plot, PlotType, collect_run_groups, ensure_plot_dirs, render_heatmaps},
    util::{
        BarChartKind, SectionStats, calculate_sectioned, make_power_state_bar_config,
        plot_bar_chart, power_energy_calculator, read_json_file,
    },
};
use eyre::{Context, Result, bail};
use futures::future::join_all;
use itertools::Itertools;
use plot_common::{default_timeseries_plot, impl_power_time_plot};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use tokio::fs::read_to_string;
use tracing::debug;
use ycsb::{Ycsb, result::YcsbMetrics};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct YcsbBasic;

#[derive(Debug, Clone)]
struct PlotEntry {
    result: YcsbMetrics,
    info: BenchParams,
    args: Ycsb,
    ssd_power: SectionedCalculation,
    cpu_power: SectionedCalculation,
}

#[derive(Debug, Clone)]
pub struct SectionedCalculation {
    pub overall: SectionStats,
    pub benchmark: SectionStats,
    pub unmount: SectionStats,
}

#[async_trait::async_trait]
#[typetag::serde]
impl Plot for YcsbBasic {
    fn required_sensors(&self) -> &'static [&'static str] {
        &["Powersensor3", "Rapl"]
    }

    async fn plot(
        &self,
        plot_type: &PlotType,
        data_path: &Path,
        plot_path: &Path,
        _config: &Config,
        bench_info: &BenchInfo,
        dirs: Vec<String>,
        settings: &Settings,
        completed_dirs: &mut Vec<String>,
    ) -> Result<()> {
        if *plot_type == PlotType::Total {
            return Ok(());
        }

        debug!("Got {} dirs", dirs.len());
        let groups = collect_run_groups(dirs, &bench_info.param_map, completed_dirs)?;
        if groups.is_empty() {
            return Ok(());
        }

        let entries = join_all(groups.iter().map(|group| {
            let run_dir = data_path.join(&group.dir);
            let dir = group.dir.clone();
            let info = group.info.clone();
            async move {
                (
                    read_json_file::<YcsbMetrics>(run_dir.join("results.json")).await,
                    read_to_string(run_dir.join("powersensor3.csv")).await,
                    read_to_string(run_dir.join("rapl.csv")).await,
                    read_to_string(run_dir.join("markers.csv")).await,
                    dir,
                    info,
                )
            }
        }))
        .await;
        let ready_entries = entries
            .into_par_iter()
            .map(|item| {
                let (json, powersensor3, rapl, markers, _dir, info) = item;
                let markers = markers.context("Read markers").unwrap();
                let rapl = rapl.context("Read rapl").unwrap();
                let powersensor3 = powersensor3.context("Read powersensor3").unwrap();

                let (rapl_means, rapl_overall, _) = calculate_sectioned::<_, 2>(
                    Some(&markers),
                    &rapl,
                    &["Total"],
                    &[(0.0, settings.cpu_max_power_watts)],
                    power_energy_calculator,
                    None,
                )
                .context("Calculate rapl means")
                .unwrap();
                let (powersensor3_means, ps3_overall, _times) = calculate_sectioned::<_, 2>(
                    Some(&markers),
                    &powersensor3,
                    &["Total"],
                    &[(0.0, bench_info.device_power_states[0].0)],
                    power_energy_calculator,
                    None,
                )
                .context("Calculate powersensor3 means")
                .unwrap();

                PlotEntry {
                    result: json.context("Read results json").unwrap(),
                    args: info.args.downcast_ref::<Ycsb>().unwrap().clone(),
                    info,
                    ssd_power: SectionedCalculation {
                        overall: ps3_overall,
                        benchmark: powersensor3_means[0],
                        unmount: powersensor3_means[1],
                    },
                    cpu_power: SectionedCalculation {
                        overall: rapl_overall,
                        benchmark: rapl_means[0],
                        unmount: rapl_means[1],
                    },
                }
            })
            .collect::<Vec<_>>();

        let latency_dir = plot_path.join("latency");
        let power_dir = plot_path.join("power");
        let iops_dir = plot_path.join("iops");
        let efficiency_dir = plot_path.join("efficiency");
        ensure_plot_dirs(&[
            latency_dir.clone(),
            power_dir.clone(),
            iops_dir.clone(),
            efficiency_dir.clone(),
        ])
        .await?;

        let experiment_name = ready_entries[0].info.name.clone();
        let plot_jobs: Vec<(
            Vec<PlotEntry>,
            &Settings,
            PathBuf,
            &str,
            &str,
            fn(&PlotEntry) -> Option<f64>,
        )> = vec![
            (
                ready_entries.clone(),
                settings,
                iops_dir.join(format!("{experiment_name}.pdf")),
                "throughput",
                "kOPS/s",
                |data| data.result.throughput_ops_sec.as_ref().map(|x| x / 1000.0),
            ),
            (
                ready_entries.clone(),
                settings,
                latency_dir.join(format!("{experiment_name}-read.pdf")),
                "latency",
                "ms",
                |data| data.result.read.as_ref().map(|x| x.p99_latency_us / 1000.0),
            ),
            (
                ready_entries.clone(),
                settings,
                latency_dir.join(format!("{experiment_name}-update.pdf")),
                "latency",
                "ms",
                |data| {
                    data.result
                        .update
                        .as_ref()
                        .map(|x| x.p99_latency_us / 1000.0)
                },
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-cpu.pdf")),
                "power",
                "W",
                |data| data.cpu_power.benchmark.power,
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-ssd.pdf")),
                "power",
                "W",
                |data| data.ssd_power.benchmark.power,
            ),
        ];

        let results = plot_jobs
            .into_par_iter()
            .map(|x| self.bar_plot(x.0, x.1, x.2, x.3, x.4, x.5, bench_info))
            .collect::<Vec<_>>();
        for item in results {
            item?;
        }

        self.efficiency(ready_entries.clone(), settings, &efficiency_dir)
            .await?;
        Ok(())
    }
}

impl YcsbBasic {
    fn bar_plot(
        &self,
        ready_entries: Vec<PlotEntry>,
        settings: &Settings,
        filepath: PathBuf,
        plotting_file: &str,
        x_label: &str,
        get_value: fn(&PlotEntry) -> Option<f64>,
        bench_info: &BenchInfo,
    ) -> Result<()> {
        let num_power_states = settings.nvme_power_states.clone().unwrap_or(vec![0]).len();
        let mut results = vec![vec![]; num_power_states];
        let vars = ready_entries
            .iter()
            .map(|x| format!("{:?}", x.args._ycsb_op_type.as_ref().unwrap()))
            .collect::<HashSet<_>>();
        let data = vars.into_iter().sorted();
        let order: HashMap<String, usize> = data.clone().enumerate().map(|(x, y)| (y, x)).collect();
        let labels: Vec<String> = data.map(|x| x.to_string()).collect();

        let experiment_name = ready_entries[0].info.name.clone();

        for item in ready_entries {
            let mean = match get_value(&item) {
                Some(x) => x,
                None => continue,
            };
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
                    .get(&format!(
                        "{:?}",
                        entry.0.args._ycsb_op_type.as_ref().unwrap()
                    ))
                    .unwrap()
            });
        }

        let results = results
            .iter()
            .map(|x| x.iter().map(|x| x.1).collect::<Vec<_>>())
            .collect::<Vec<_>>();

        let chart_kind = match plotting_file {
            "throughput" => BarChartKind::Throughput,
            "power" => BarChartKind::Power,
            other => bail!("Unsupported plotting file {other}"),
        };
        let config = make_power_state_bar_config(chart_kind, x_label, &experiment_name, None);
        plot_bar_chart(&filepath, results, labels, config, bench_info)
    }

    async fn efficiency(
        &self,
        ready_entries: Vec<PlotEntry>,
        settings: &Settings,
        plot_path: &Path,
    ) -> Result<()> {
        let num_power_states = settings.nvme_power_states.clone().unwrap_or(vec![0]).len();
        let (order, labels) = self.get_order_labels(ready_entries.clone());
        let mut iops_j_overall = vec![vec![0f64; num_power_states]; order.len()];
        let mut iops_j_benchmark = iops_j_overall.clone();
        let mut iops_j_unmount = iops_j_overall.clone();
        let experiment_name = ready_entries[0].info.name.clone();

        let results = ready_entries
            .par_iter()
            .map(|item| {
                let x = *order
                    .get(&format!(
                        "{:?} {} {:?}",
                        item.args.fs,
                        item.args.workload_file,
                        item.args._ycsb_op_type.as_ref().unwrap()
                    ))
                    .unwrap();
                let y = if item.info.power_state == -1 {
                    0
                } else {
                    item.info.power_state
                } as usize;

                let throughput = item.result.throughput_ops_sec.as_ref().unwrap() / 1000.0;
                (
                    x,
                    y,
                    throughput / item.ssd_power.overall.power.unwrap(),
                    throughput / item.ssd_power.benchmark.power.unwrap(),
                    throughput / item.ssd_power.unmount.power.unwrap(),
                )
            })
            .collect::<Vec<_>>();
        for item in results {
            let x = item.0;
            let y = item.1;
            iops_j_overall[x][y] = item.2;
            iops_j_benchmark[x][y] = item.3;
            iops_j_unmount[x][y] = item.4;
        }

        let jobs = vec![
            HeatmapJob {
                filepath: plot_path.join(format!("{}-iops-j-overall.pdf", &experiment_name)),
                data: iops_j_overall,
                title: "kIOPS/J",
                x_label: "overall",
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-iops-j-benchmark.pdf", &experiment_name)),
                data: iops_j_benchmark,
                title: "kIOPS/J",
                x_label: "benchmark",
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-iops-j-unmount.pdf", &experiment_name)),
                data: iops_j_unmount,
                title: "kIOPS/J",
                x_label: "unmount",
                reverse: false,
            },
        ];

        render_heatmaps(&experiment_name, &labels, plot_path, &jobs)
    }

    fn get_order_labels(
        &self,
        ready_entries: Vec<PlotEntry>,
    ) -> (HashMap<String, usize>, Vec<String>) {
        let vars = ready_entries
            .iter()
            .map(|x| {
                (
                    &x.args.fs,
                    x.args.workload_file.clone(),
                    x.args._ycsb_op_type.clone(),
                )
            })
            .collect::<HashSet<_>>();
        let data = vars.into_iter().sorted_by(|a, b| {
            let fs_cmp = a.0.ord().cmp(&b.0.ord());
            if fs_cmp != std::cmp::Ordering::Equal {
                fs_cmp
            } else {
                a.2.as_ref().unwrap().cmp(b.2.as_ref().unwrap())
            }
        });
        let order: HashMap<String, usize> = data
            .clone()
            .map(|x| format!("{:?} {} {:?}", x.0, x.1, x.2.as_ref().unwrap()))
            .enumerate()
            .map(|(x, y)| (y, x))
            .collect();
        let labels: Vec<String> = data
            .map(|x| format!("{:?} {} {:?}", x.0, x.1, x.2.as_ref().unwrap()))
            .collect();
        (order, labels)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct YcsbPowerTime {
    pub offset: Option<usize>,
}

impl_power_time_plot!(
    YcsbPowerTime,
    Ycsb,
    |cfg: &Ycsb| format!("{:?}-{:?}", cfg._ycsb_op_type.as_ref().unwrap(), cfg.fs),
    |cfg: &Ycsb| cfg.fs.clone()
);
