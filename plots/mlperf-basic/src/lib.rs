use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use common::{
    MB_TO_MIB,
    bench::{BenchInfo, BenchParams},
    config::{Config, Settings},
    plot::{HeatmapJob, Plot, PlotType, collect_run_groups, ensure_plot_dirs, render_heatmaps},
    util::{
        BarChartKind, SectionStats, calculate_sectioned, make_power_state_bar_config,
        plot_bar_chart, plot_time_series, power_energy_calculator,
    },
};
use eyre::{Context, ContextCompat, Result, bail};
use futures::future::join_all;
use itertools::Itertools;
use mlperf::{
    Mlperf,
    result::{MlperfMetrics, find_summary},
};
use plot_common::default_timeseries_plot;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use tokio::fs::read_to_string;
use tracing::debug;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MlperfBasic;

#[derive(Debug, Clone)]
struct PlotEntry {
    result: MlperfMetrics,
    info: BenchParams,
    args: Mlperf,
    ssd_power: SectionStats,
    cpu_power: SectionStats,
}

#[async_trait::async_trait]
#[typetag::serde]
impl Plot for MlperfBasic {
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

        async fn read_results_json(folder: PathBuf) -> Result<MlperfMetrics> {
            let summary_file_path =
                find_summary(&folder.join("training")).context("Could not find summary.json")?;
            let data = read_to_string(summary_file_path).await?;
            serde_json::from_str(&data).context("Parse summary.json")
        }

        let entries = join_all(groups.iter().map(|group| {
            let run_dir = data_path.join(&group.dir);
            let dir = group.dir.clone();
            let info = group.info.clone();
            async move {
                (
                    read_results_json(run_dir.clone()).await,
                    read_to_string(run_dir.join("powersensor3.csv")).await,
                    read_to_string(run_dir.join("rapl.csv")).await,
                    dir,
                    info,
                )
            }
        }))
        .await;
        let ready_entries = entries
            .into_par_iter()
            .map(|item| {
                let (json, powersensor3, rapl, _dir, info) = item;
                let rapl = rapl.context("Read rapl").unwrap();
                let powersensor3 = powersensor3.context("Read powersensor3").unwrap();

                let (_, rapl_overall, _) = calculate_sectioned::<_, 0>(
                    None,
                    &rapl,
                    &["Total"],
                    &[(0.0, settings.cpu_max_power_watts)],
                    power_energy_calculator,
                    None,
                )
                .context("Calculate rapl means")
                .unwrap();
                let (_, ps3_overall, _times) = calculate_sectioned::<_, 0>(
                    None,
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
                    args: info.args.downcast_ref::<Mlperf>().unwrap().clone(),
                    info,
                    ssd_power: ps3_overall,
                    cpu_power: rapl_overall,
                }
            })
            .collect::<Vec<_>>();

        let experiment_name = ready_entries[0].info.name.clone();
        let throughput_dir = plot_path.join("throughput");
        let efficiency_dir = plot_path.join("efficiency");
        let power_dir = plot_path.join("power");
        ensure_plot_dirs(&[
            throughput_dir.clone(),
            efficiency_dir.clone(),
            power_dir.clone(),
        ])
        .await?;

        let plot_jobs: Vec<(
            Vec<PlotEntry>,
            &Settings,
            PathBuf,
            &str,
            &str,
            fn(&PlotEntry) -> f64,
        )> = vec![
            (
                ready_entries.clone(),
                settings,
                throughput_dir.join(format!("{experiment_name}-train.pdf")),
                "throughput",
                "MiB/s",
                |data| data.result.metric.train_io_mean_mb_per_second * MB_TO_MIB,
            ),
            (
                ready_entries.clone(),
                settings,
                throughput_dir.join(format!("{experiment_name}-samples.pdf")),
                "throughput",
                "samples/s",
                |data| data.result.metric.train_throughput_mean_samples_per_second,
            ),
            (
                ready_entries.clone(),
                settings,
                throughput_dir.join(format!("{experiment_name}-utilization.pdf")),
                "throughput",
                "%",
                |data| data.result.metric.train_au_mean_percentage,
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-cpu.pdf")),
                "power",
                "%",
                |data| data.cpu_power.power.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-ssd.pdf")),
                "power",
                "%",
                |data| data.ssd_power.power.unwrap(),
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

impl MlperfBasic {
    fn bar_plot(
        &self,
        ready_entries: Vec<PlotEntry>,
        settings: &Settings,
        filepath: PathBuf,
        plotting_file: &str,
        x_label: &str,
        get_value: fn(&PlotEntry) -> f64,
        bench_info: &BenchInfo,
    ) -> Result<()> {
        let num_power_states = settings.nvme_power_states.clone().unwrap_or(vec![0]).len();
        let mut results = vec![vec![]; num_power_states];
        let (order, labels) = self.get_order_labels(ready_entries.clone());

        let experiment_name = ready_entries[0].info.name.clone();

        for item in ready_entries {
            let mean = get_value(&item);
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
                    .get(&format!("{}", entry.0.args.n_accelerators[0]))
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
        let mut iops_j = vec![vec![0f64; num_power_states]; order.len()];
        let mut bytes_j = iops_j.clone();
        let mut bytes_j_ssd = iops_j.clone();
        let experiment_name = ready_entries[0].info.name.clone();

        let results = ready_entries
            .par_iter()
            .map(|item| {
                let ops = item.result.metric.train_throughput_mean_samples_per_second;
                let throughput = item.result.metric.train_io_mean_mb_per_second * MB_TO_MIB;
                let x = *order
                    .get(&format!("{}", item.args.n_accelerators[0],))
                    .unwrap();
                let y = if item.info.power_state == -1 {
                    0
                } else {
                    item.info.power_state
                } as usize;

                let ssd_power = item.ssd_power.power.unwrap();
                let cpu_power = item.cpu_power.power.unwrap();
                (
                    x,
                    y,
                    ops / (ssd_power + cpu_power),
                    throughput / (ssd_power + cpu_power),
                    throughput / ssd_power,
                )
            })
            .collect::<Vec<_>>();
        for item in results {
            let x = item.0;
            let y = item.1;
            iops_j[x][y] = item.2;
            bytes_j[x][y] = item.3;
            bytes_j_ssd[x][y] = item.4;
        }

        let jobs = vec![
            HeatmapJob {
                filepath: plot_path.join(format!("{}-iops-j.pdf", &experiment_name)),
                data: iops_j,
                title: "Samples/J",
                x_label: "overall",
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-bytes-j+cpu.pdf", &experiment_name)),
                data: bytes_j,
                title: "MiB/J",
                x_label: "overall",
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-bytes-j.pdf", &experiment_name)),
                data: bytes_j_ssd,
                title: "MiB/J",
                x_label: "overall",
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
            .map(|x| (&x.args.n_accelerators[0],))
            .collect::<HashSet<_>>();
        let data = vars.into_iter().sorted_by(|a, b| a.0.cmp(b.0));
        let order: HashMap<String, usize> = data
            .clone()
            .map(|x| format!("{}", x.0))
            .enumerate()
            .map(|(x, y)| (y, x))
            .collect();
        let labels: Vec<String> = data.map(|x| format!("{}", x.0)).collect();
        (order, labels)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MlperfPowerTime {
    pub offset: Option<usize>,
}

#[async_trait::async_trait]
#[typetag::serde]
impl Plot for MlperfPowerTime {
    fn required_sensors(&self) -> &'static [&'static str] {
        &["Powersensor3", "Rapl", "Sysinfo"]
    }

    async fn plot(
        &self,
        plot_type: &PlotType,
        data_path: &Path,
        plot_path: &Path,
        _config_yaml: &Config,
        info: &BenchInfo,
        dirs: Vec<String>,
        _: &Settings,
        completed_dirs: &mut Vec<String>,
    ) -> Result<()> {
        if *plot_type == PlotType::Total {
            return Ok(());
        }

        let groups = collect_run_groups(dirs, &info.param_map, completed_dirs)?;
        if groups.is_empty() {
            return Ok(());
        }

        let dir = plot_path.join("mlperf_time");
        let inner_dir = dir.join(&groups[0].info.name);
        let dir_list = vec![dir.clone(), inner_dir.clone(), inner_dir.join("plot_data")];
        ensure_plot_dirs(&dir_list).await?;

        for group in &groups {
            self.mlperf_time(data_path.join(&group.dir), &inner_dir, &group.info, info)?;
        }
        Ok(())
    }
}

impl MlperfPowerTime {
    fn mlperf_time(
        &self,
        data_path: PathBuf,
        plot_path: &Path,
        info: &BenchParams,
        bench_info: &BenchInfo,
    ) -> Result<()> {
        let config = info.args.downcast_ref::<Mlperf>().unwrap();
        let name = format!(
            "{}-ps{}-{}",
            info.name, info.power_state, config.n_accelerators[0]
        );

        plot_time_series(
            default_timeseries_plot(
                default_benches::BenchKind::Mlperf,
                plot_path.to_path_buf(),
                data_path,
                name,
                bench_info,
            )
            .with_offset(self.offset),
        )?;
        Ok(())
    }
}
