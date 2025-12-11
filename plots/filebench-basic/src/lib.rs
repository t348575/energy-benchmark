use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
};

use common::{
    bench::{BenchInfo, BenchParams},
    config::{Config, Settings},
    plot::{HeatmapJob, Plot, PlotType, collect_run_groups, ensure_dirs, render_heatmaps},
    util::{
        BarChartKind, Filesystem, SectionStats, calculate_sectioned, make_power_state_bar_config,
        parse_data_size, parse_trace, plot_bar_chart, plot_time_series, power_energy_calculator,
        read_json_file, write_csv,
    },
};
use eyre::{Context, Result, bail};
use filebench::{Filebench, result::FilebenchSummary};
use futures::future::join_all;
use itertools::Itertools;
use plot_common::default_timeseries_plot;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use tokio::fs::read_to_string;
use tracing::debug;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilebenchBasic {
    var_name: String,
}

#[derive(Debug, Clone)]
struct PlotEntry {
    result: FilebenchSummary,
    info: BenchParams,
    args: Filebench,
    ssd_power: SectionedCalculation,
    cpu_power: SectionedCalculation,
    server_power: SectionedCalculation,
    _times: [usize; 4],
}

#[async_trait::async_trait]
#[typetag::serde]
impl Plot for FilebenchBasic {
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
                    read_json_file::<FilebenchSummary>(run_dir.join("results.json")).await,
                    read_to_string(run_dir.join("powersensor3.csv")).await,
                    read_to_string(run_dir.join("rapl.csv")).await,
                    read_to_string(run_dir.join("netio-http.csv")).await,
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
                let (json, powersensor3, rapl, system, markers, _dir, info) = item;
                let markers = markers.context("Read markers").unwrap();
                let rapl = rapl.context("Read rapl").unwrap();
                let powersensor3 = powersensor3.context("Read powersensor3").unwrap();
                let system = system.context("Read system power").unwrap();

                let (rapl_means, rapl_overall, _) = calculate_sectioned::<_, 4>(
                    Some(&markers),
                    &rapl,
                    &["Total"],
                    &[(0.0, settings.cpu_max_power_watts)],
                    power_energy_calculator,
                )
                .context("Calculate rapl means")
                .unwrap();

                let (powersensor3_means, ps3_overall, times) = calculate_sectioned::<_, 4>(
                    Some(&markers),
                    &powersensor3,
                    &["Total"],
                    &[(0.0, bench_info.device_power_states[0].0)],
                    power_energy_calculator,
                )
                .context("Calculate powersensor3 means")
                .unwrap();

                let (system_means, system_overall, _) = calculate_sectioned::<_, 4>(
                    Some(&markers),
                    &system,
                    &[r#"load-\S+"#],
                    &[(0.0, settings.cpu_max_power_watts * 2.0)],
                    power_energy_calculator,
                )
                .context("Calculate system power means")
                .unwrap();

                PlotEntry {
                    result: json.context("Read results json").unwrap(),
                    args: info.args.downcast_ref::<Filebench>().unwrap().clone(),
                    info,
                    ssd_power: SectionedCalculation {
                        overall: ps3_overall,
                        init: powersensor3_means[0],
                        benchmark: powersensor3_means[1],
                        post_benchmark: powersensor3_means[2],
                    },
                    cpu_power: SectionedCalculation {
                        overall: rapl_overall,
                        init: rapl_means[0],
                        benchmark: rapl_means[1],
                        post_benchmark: rapl_means[2],
                    },
                    server_power: SectionedCalculation {
                        overall: system_overall,
                        init: system_means[0],
                        benchmark: system_means[1],
                        post_benchmark: system_means[2],
                    },
                    _times: times,
                }
            })
            .collect::<Vec<_>>();

        let throughput_dir = plot_path.join("throughput");
        let latency_dir = plot_path.join("latency");
        let power_dir_cpu = plot_path.join("power-cpu");
        let power_dir_ssd = plot_path.join("power-ssd");
        let iops_dir = plot_path.join("iops");
        let efficiency_dir = plot_path.join("efficiency");
        let dir_list = vec![
            throughput_dir.clone(),
            latency_dir.clone(),
            power_dir_cpu.clone(),
            power_dir_ssd.clone(),
            iops_dir.clone(),
            efficiency_dir.clone(),
        ];
        ensure_dirs(&dir_list).await?;

        let experiment_name = ready_entries[0].info.name.clone();
        let plot_jobs: Vec<(
            Vec<PlotEntry>,
            &Settings,
            PathBuf,
            &str,
            Option<&str>,
            &str,
            fn(&PlotEntry) -> f64,
        )> = vec![
            (
                ready_entries.clone(),
                settings,
                throughput_dir.join(format!("{experiment_name}-read.pdf")),
                "throughput",
                None,
                "MB/s",
                |data| {
                    data.result
                        .ops_stats
                        .iter()
                        .filter(|x| x.name.starts_with("readfile"))
                        .map(|x| x.mb_per_sec)
                        .sum()
                },
            ),
            (
                ready_entries.clone(),
                settings,
                throughput_dir.join(format!("{experiment_name}-write.pdf")),
                "throughput",
                None,
                "MB/s",
                |data| {
                    let write_names = ["writefile", "wrtfile", "append", "fsync"];
                    data.result
                        .ops_stats
                        .iter()
                        .filter(|x| {
                            write_names
                                .iter()
                                .any(|write_name| x.name.starts_with(write_name))
                        })
                        .map(|x| x.mb_per_sec)
                        .sum()
                },
            ),
            (
                ready_entries.clone(),
                settings,
                iops_dir.join(format!("{experiment_name}.pdf")),
                "throughput",
                None,
                "kOPS/s",
                |data| data.result.summary.ops_per_sec / 1000.0,
            ),
            (
                ready_entries.clone(),
                settings,
                latency_dir.join(format!("{experiment_name}.pdf")),
                "latency",
                None,
                "ms",
                |data| data.result.summary.latency_ms,
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir_ssd.join(format!("{experiment_name}-overall.pdf",)),
                "power",
                Some("SSD"),
                "ms",
                |data| data.ssd_power.overall.power_mean.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir_ssd.join(format!("{experiment_name}-init.pdf",)),
                "power",
                Some("CPU + DRAM"),
                "ms",
                |data| data.ssd_power.init.power_mean.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir_ssd.join(format!("{experiment_name}-benchmark.pdf",)),
                "power",
                Some("CPU + DRAM"),
                "ms",
                |data| data.ssd_power.benchmark.power_mean.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir_ssd.join(format!("{experiment_name}-post-benchmark.pdf",)),
                "power",
                Some("CPU + DRAM"),
                "ms",
                |data| data.ssd_power.post_benchmark.power_mean.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir_cpu.join(format!("{experiment_name}-overall.pdf",)),
                "power",
                Some("CPU + DRAM"),
                "ms",
                |data| data.cpu_power.overall.power_mean.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir_cpu.join(format!("{experiment_name}-init.pdf",)),
                "power",
                Some("CPU + DRAM"),
                "ms",
                |data| data.cpu_power.init.power_mean.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir_cpu.join(format!("{experiment_name}-benchmark.pdf",)),
                "power",
                Some("CPU + DRAM"),
                "ms",
                |data| data.cpu_power.benchmark.power_mean.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir_cpu.join(format!("{experiment_name}-post-benchmark.pdf",)),
                "power",
                Some("CPU + DRAM"),
                "ms",
                |data| data.cpu_power.post_benchmark.power_mean.unwrap(),
            ),
        ];

        let results = plot_jobs
            .into_par_iter()
            .map(|x| self.bar_plot(x.0, x.1, x.2, x.3, x.4, x.5, x.6, bench_info))
            .collect::<Vec<_>>();
        for item in results {
            item?;
        }

        self.efficiency(ready_entries.clone(), settings, &efficiency_dir)
            .await?;
        Ok(())
    }
}

impl FilebenchBasic {
    fn bar_plot(
        &self,
        ready_entries: Vec<PlotEntry>,
        settings: &Settings,
        filepath: PathBuf,
        plotting_file: &str,
        y_name: Option<&str>,
        x_label: &str,
        get_mean: fn(&PlotEntry) -> f64,
        bench_info: &BenchInfo,
    ) -> Result<()> {
        let num_power_states = settings.nvme_power_states.clone().unwrap_or(vec![0]).len();
        let mut results = vec![vec![]; num_power_states];
        let (order, labels) = self.get_order_labels(ready_entries.clone());

        let experiment_name = ready_entries[0].info.name.clone();

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
                    .get(&format!(
                        "{:?} {}",
                        entry.0.args.fs[0],
                        entry.0.args.vars.clone().unwrap()[0]
                            .get(&self.var_name)
                            .unwrap_or(&"default".to_string())
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
            "latency" => BarChartKind::Latency,
            "power" => BarChartKind::Power,
            other => bail!("Unsupported plotting file {other}"),
        };
        let config = make_power_state_bar_config(chart_kind, x_label, &experiment_name, y_name);
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
        let mut iops_j_init = iops_j_overall.clone();
        let mut iops_j_benchmark = iops_j_overall.clone();
        let mut iops_j_benchmark_cpu = iops_j_overall.clone();
        let mut iops_j_benchmark_server = iops_j_overall.clone();
        let mut iops_j_post_benchmark = iops_j_overall.clone();
        let mut edp = iops_j_overall.clone();
        let mut bytes_j_overall = iops_j_overall.clone();
        let mut bytes_j_init = iops_j_overall.clone();
        let mut bytes_j_benchmark = iops_j_overall.clone();
        let mut bytes_j_post_benchmark = iops_j_overall.clone();
        let experiment_name = ready_entries[0].info.name.clone();

        let results = ready_entries
            .par_iter()
            .map(|item| {
                let iops = item.result.summary.ops_per_sec;
                let bytes = item.result.summary.mb_per_sec;
                let latency = item.result.summary.latency_ms;
                let x = *order
                    .get(&format!(
                        "{:?} {}",
                        item.args.fs[0],
                        item.args.vars.clone().unwrap()[0]
                            .get(&self.var_name)
                            .unwrap_or(&"default".to_string())
                    ))
                    .unwrap();
                let y = if item.info.power_state == -1 {
                    0
                } else {
                    item.info.power_state
                } as usize;

                (
                    x,
                    y,
                    iops / item.ssd_power.overall.power_mean.unwrap(),
                    iops / item.ssd_power.init.power_mean.unwrap(),
                    iops / item.ssd_power.benchmark.power_mean.unwrap(),
                    iops / item.cpu_power.benchmark.power_mean.unwrap(),
                    iops / item.server_power.benchmark.power_mean.unwrap(),
                    iops / item.ssd_power.post_benchmark.power_mean.unwrap(),
                    bytes / item.ssd_power.overall.power_mean.unwrap(),
                    bytes / item.ssd_power.init.power_mean.unwrap(),
                    bytes / item.ssd_power.benchmark.power_mean.unwrap(),
                    bytes / item.ssd_power.post_benchmark.power_mean.unwrap(),
                    item.ssd_power.benchmark.power_mean.unwrap() * latency.powi(2),
                )
            })
            .collect::<Vec<_>>();
        for item in results {
            let x = item.0;
            let y = item.1;
            iops_j_overall[x][y] = item.2;
            iops_j_init[x][y] = item.3;
            iops_j_benchmark[x][y] = item.4;
            iops_j_benchmark_cpu[x][y] = item.5;
            iops_j_benchmark_server[x][y] = item.6;
            iops_j_post_benchmark[x][y] = item.7;
            bytes_j_overall[x][y] = item.8;
            bytes_j_init[x][y] = item.9;
            bytes_j_benchmark[x][y] = item.10;
            bytes_j_post_benchmark[x][y] = item.11;
            edp[x][y] = item.12;
        }

        let jobs = vec![
            HeatmapJob {
                filepath: plot_path.join(format!("{}-iops-j-overall.pdf", &experiment_name)),
                data: iops_j_overall,
                title: "IOPS/J",
                x_label: "overall",
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-iops-j-init.pdf", &experiment_name)),
                data: iops_j_init,
                title: "IOPS/J",
                x_label: "init",
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-iops-j-cpu-benchmark.pdf", &experiment_name)),
                data: iops_j_benchmark_cpu,
                title: "IOPS/J",
                x_label: "benchmark",
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path
                    .join(format!("{}-iops-j-server-benchmark.pdf", &experiment_name)),
                data: iops_j_benchmark_server,
                title: "IOPS/J",
                x_label: "benchmark",
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-iops-j-benchmark.pdf", &experiment_name)),
                data: iops_j_benchmark,
                title: "IOPS/J",
                x_label: "benchmark",
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-iops-j-post-benchmark.pdf", &experiment_name)),
                data: iops_j_post_benchmark,
                title: "IOPS/J",
                x_label: "post-benchmark",
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-bytes-j-overall.pdf", &experiment_name)),
                data: bytes_j_overall,
                title: "Bytes/J",
                x_label: "overall",
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-bytes-j-init.pdf", &experiment_name)),
                data: bytes_j_init,
                title: "Bytes/J",
                x_label: "init",
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-bytes-j-benchmark.pdf", &experiment_name)),
                data: bytes_j_benchmark,
                title: "Bytes/J",
                x_label: "benchmark",
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path
                    .join(format!("{}-bytes-j-post-benchmark.pdf", &experiment_name)),
                data: bytes_j_post_benchmark,
                title: "Bytes/J",
                x_label: "post-benchmark",
                reverse: false,
            },
            HeatmapJob {
                filepath: plot_path.join(format!("{}-edp.pdf", &experiment_name)),
                data: edp,
                title: "EDP",
                x_label: "edp",
                reverse: true,
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
                    &x.args.fs[0],
                    x.args.vars.as_ref().unwrap()[0]
                        .get(&self.var_name)
                        .unwrap_or(&"default".to_string())
                        .clone(),
                )
            })
            .collect::<HashSet<_>>();
        let data = vars.into_iter().sorted_by(|a, b| {
            let fs_cmp = a.0.ord().cmp(&b.0.ord());
            if fs_cmp != std::cmp::Ordering::Equal {
                fs_cmp
            } else {
                match self.var_name.as_str() {
                    "meanfilesize" => {
                        let a_size = parse_data_size(&a.1).unwrap();
                        let b_size = parse_data_size(&b.1).unwrap();
                        a_size.cmp(&b_size)
                    }
                    "nfiles" | "nthreads" => {
                        a.1.parse::<usize>()
                            .unwrap()
                            .cmp(&b.1.parse::<usize>().unwrap())
                    }
                    _ => unimplemented!(),
                }
            }
        });
        let order: HashMap<String, usize> = data
            .clone()
            .map(|x| format!("{:?} {}", x.0, x.1))
            .enumerate()
            .map(|(x, y)| (y, x))
            .collect();
        let labels: Vec<String> = data.map(|x| format!("{:?} {}", x.0, x.1)).collect();
        (order, labels)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilebenchPowerTime {
    pub var_name: String,
    pub offset: Option<usize>,
}

#[async_trait::async_trait]
#[typetag::serde]
impl Plot for FilebenchPowerTime {
    fn required_sensors(&self) -> &'static [&'static str] {
        &["Powersensor3", "Rapl", "Sysinfo"]
    }

    async fn plot(
        &self,
        plot_type: &PlotType,
        data_path: &Path,
        plot_path: &Path,
        _config_yaml: &Config,
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

        let dir = plot_path.join("filebench_time");
        let inner_dir = dir.join(&groups[0].info.name);
        let dir_list = vec![dir.clone(), inner_dir.clone(), inner_dir.join("plot_data")];
        ensure_dirs(&dir_list).await?;

        let results = groups
            .par_iter()
            .map(|data| {
                self.filebench_time(
                    data_path.join(&data.dir),
                    &inner_dir,
                    &data.info,
                    bench_info,
                )
            })
            .collect::<Vec<_>>();
        for item in results {
            item?;
        }
        Ok(())
    }
}

impl FilebenchPowerTime {
    fn filebench_time(
        &self,
        data_path: PathBuf,
        plot_path: &Path,
        info: &BenchParams,
        bench_info: &BenchInfo,
    ) -> Result<()> {
        let config = info.args.downcast_ref::<Filebench>().unwrap();
        let vars = &config.vars.as_ref().unwrap()[0];
        let fs = config.fs.first().unwrap_or(&Filesystem::None);
        let name = format!(
            "{}-ps{}-{}-{:?}",
            info.name,
            info.power_state,
            vars.get(&self.var_name).unwrap(),
            fs
        );

        let trace_file = data_path.join("trace.out");
        if trace_file.exists() {
            let trace = parse_trace(&std::fs::File::open(&trace_file)?, fs)?;
            write_csv(
                &plot_path.join("plot_data").join(format!("{name}.csv")),
                &trace,
            )?;
        }

        plot_time_series(
            default_timeseries_plot(
                default_benches::BenchKind::Filebench,
                plot_path.to_path_buf(),
                data_path,
                name,
                bench_info,
            )
            .with_offset(self.offset.unwrap_or_default()),
        )?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SectionedCalculation {
    pub overall: SectionStats,
    pub init: SectionStats,
    pub benchmark: SectionStats,
    pub post_benchmark: SectionStats,
}
