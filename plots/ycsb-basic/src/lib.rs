use std::{
    collections::{HashMap, HashSet},
    fs::write,
    path::{Path, PathBuf},
};

use common::{
    bench::BenchmarkInfo,
    config::{Config, Settings},
    plot::{Plot, PlotType},
    util::{
        SectionStats, calculate_sectioned, parse_trace, plot_python, power_energy_calculator,
        write_csv,
    },
};
use eyre::{Context, ContextCompat, Result};
use futures::future::join_all;
use itertools::Itertools;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use tokio::fs::{create_dir_all, read_to_string};
use tracing::debug;
use ycsb::{OpType, Ycsb, result::YcsbMetrics};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct YcsbBasic;

#[derive(Debug, Clone)]
struct PlotEntry {
    result: YcsbMetrics,
    info: BenchmarkInfo,
    args: Ycsb,
    ssd_power: SectionedCalculation,
    cpu_power: SectionedCalculation,
    times: [usize; 2],
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
        info: &HashMap<String, BenchmarkInfo>,
        dirs: Vec<String>,
        settings: &Settings,
        completed_dirs: &mut Vec<String>,
    ) -> Result<()> {
        if *plot_type == PlotType::Total {
            return Ok(());
        }

        debug!("Got {} dirs", dirs.len());
        let mut groups = HashMap::new();
        for run in dirs {
            let item = info.get(&run).context("No info for run")?;
            let key = (item.name.clone(), item.power_state, item.hash.clone());
            groups.entry(key).or_insert_with(|| {
                completed_dirs.push(run.clone());
                (run, item.clone())
            });
        }

        if groups.is_empty() {
            return Ok(());
        }

        async fn read_results_json(folder: PathBuf) -> Result<YcsbMetrics> {
            let data = read_to_string(folder.join("results.json")).await?;
            serde_json::from_str(&data).context("Parse results.json")
        }

        let entries = groups
            .drain()
            .map(|(_, (folder, info))| {
                let specific_data_path = data_path.join(folder.clone());
                async move {
                    (
                        read_results_json(data_path.join(folder.clone())).await,
                        read_to_string(specific_data_path.join("powersensor3.csv")).await,
                        read_to_string(specific_data_path.join("rapl.csv")).await,
                        read_to_string(specific_data_path.join("markers.csv")).await,
                        folder,
                        info,
                    )
                }
            })
            .collect::<Vec<_>>();

        let entries = join_all(entries).await;
        let ready_entries = entries
            .into_par_iter()
            .map(|item| {
                let (json, powersensor3, rapl, markers, _dir, info) = item;
                let markers = markers.context("Read markers").unwrap();
                let rapl = rapl.context("Read rapl").unwrap();
                let powersensor3 = powersensor3.context("Read powersensor3").unwrap();

                let (rapl_means, rapl_overall, _) = calculate_sectioned::<_, 2>(
                    &markers,
                    &rapl,
                    "Total",
                    0.0,
                    200.0,
                    power_energy_calculator,
                )
                .context("Calculate rapl means")
                .unwrap();
                let (powersensor3_means, ps3_overall, times) = calculate_sectioned::<_, 2>(
                    &markers,
                    &powersensor3,
                    "Total",
                    0.0,
                    8.5,
                    power_energy_calculator,
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
                    times,
                }
            })
            .collect::<Vec<_>>();

        let experiment_name = ready_entries[0].info.name.clone();
        let iops_dir = plot_path.join("iops");
        create_dir_all(&iops_dir).await?;
        self.bar_plot(
            ready_entries.clone(),
            settings,
            iops_dir.join(format!("{}.pdf", experiment_name)),
            "throughput",
            "kOPS/s",
            |data| data.result.throughput_ops_sec,
        )?;

        let latency_dir = plot_path.join("latency");
        self.bar_plot(
            ready_entries.clone(),
            settings,
            latency_dir.join(format!("{}-read.pdf", experiment_name)),
            "latency",
            "ms",
            |data| {
                data.result
                    .read
                    .as_ref()
                    .and_then(|x| Some(x.p99_latency_us))
            },
        )?;
        self.bar_plot(
            ready_entries.clone(),
            settings,
            latency_dir.join(format!("{}-update.pdf", experiment_name)),
            "latency",
            "ms",
            |data| {
                data.result
                    .update
                    .as_ref()
                    .and_then(|x| Some(x.p99_latency_us))
            },
        )?;

        let power_dir = plot_path.join("power");
        self.bar_plot(
            ready_entries.clone(),
            settings,
            power_dir.join(format!("{}-cpu.pdf", experiment_name)),
            "power",
            "ms",
            |data| data.cpu_power.benchmark.power,
        )?;
        self.bar_plot(
            ready_entries.clone(),
            settings,
            power_dir.join(format!("{}-ssd.pdf", experiment_name)),
            "power",
            "ms",
            |data| data.ssd_power.benchmark.power,
        )?;

        let efficiency_dir = plot_path.join("efficiency");
        create_dir_all(&efficiency_dir).await?;
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
    ) -> Result<()> {
        let num_power_states = settings.nvme_power_states.clone().unwrap_or(vec![0]).len();
        let mut results = vec![vec![]; num_power_states];
        let vars = ready_entries
            .iter()
            .map(|x| format!("{:?}", x.args._ycsb_op_type.as_ref().unwrap()))
            .collect::<HashSet<_>>();
        let data = vars.into_iter().sorted();
        let order: HashMap<String, usize> = data.clone().enumerate().map(|(x, y)| (y, x)).collect();
        let labels: Vec<String> = data.collect();

        let experiment_name = ready_entries[0].info.name.clone();

        for item in ready_entries {
            let mean = match get_value(&item) {
                Some(x) => x,
                None => continue,
            } / 1000.0;
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

        let plot_data_dir = filepath.parent().unwrap().join("plot_data");
        if !plot_data_dir.exists() {
            std::fs::create_dir_all(&plot_data_dir)?;
        }
        let plot_data_file = plot_data_dir.join(format!(
            "{}.json",
            &filepath.file_stem().unwrap().to_str().unwrap()
        ));
        write(&plot_data_file, &serde_json::to_string(&results)?)?;

        let labels = labels.join(",");
        plot_python(
            plotting_file,
            &[
                ("--data", plot_data_file.to_str().unwrap()),
                ("--filepath", filepath.to_str().unwrap()),
                ("--x_label_name", x_label),
                ("--experiment_name", &experiment_name),
                ("--labels", &labels),
            ],
        )?;
        Ok(())
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
                let iops = item.result.throughput_ops_sec.unwrap();
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

                let benchmark_time = (item.times[1] - item.times[0]) as f64 / 1000.0;
                (
                    x,
                    y,
                    iops / ((item.ssd_power.overall.energy.unwrap()
                        + item.cpu_power.overall.energy.unwrap())
                        / benchmark_time),
                    iops / ((item.ssd_power.benchmark.energy.unwrap()
                        + item.cpu_power.benchmark.energy.unwrap())
                        / benchmark_time),
                    iops / ((item.ssd_power.unmount.energy.unwrap()
                        + item.cpu_power.unmount.energy.unwrap())
                        / benchmark_time),
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
                plot_path.join(format!("{}-iops-j-overall.pdf", &experiment_name)),
                iops_j_overall,
                "IOPS/J",
                "overall",
                false,
            ),
            (
                plot_path.join(format!("{}-iops-j-benchmark.pdf", &experiment_name)),
                iops_j_benchmark,
                "IOPS/J",
                "benchmark",
                false,
            ),
            (
                plot_path.join(format!("{}-iops-j-unmount.pdf", &experiment_name)),
                iops_j_unmount,
                "IOPS/J",
                "unmount",
                false,
            ),
        ];

        let results = jobs
            .par_iter()
            .map(|(filepath, data, title, x_label, reverse)| {
                let data_file = write_json(&data, filepath, &plot_data_dir).unwrap();
                plot_python(
                    "efficiency",
                    &[
                        ("--data", data_file.to_str().unwrap()),
                        ("--filepath", filepath.to_str().unwrap()),
                        ("--col_labels", &labels.join(",")),
                        ("--x_label", x_label),
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
pub struct YcsbPowerTime {
    pub offset: Option<usize>,
}

#[async_trait::async_trait]
#[typetag::serde]
impl Plot for YcsbPowerTime {
    fn required_sensors(&self) -> &'static [&'static str] {
        &["Powersensor3", "Rapl", "Sysinfo"]
    }

    async fn plot(
        &self,
        plot_type: &PlotType,
        data_path: &Path,
        plot_path: &Path,
        _config_yaml: &Config,
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

        let dir = plot_path.join("ycsb_time");
        let inner_dir = dir.join(&entries[0].1.name);
        create_dir_all(&inner_dir).await?;
        create_dir_all(inner_dir.join("plot_data")).await?;
        let results = entries
            .into_iter()
            .map(|data| self.ycsb_time(data_path.join(data.0.clone()), &inner_dir, &data.1))
            .collect::<Vec<_>>();
        for item in results {
            item?;
        }
        Ok(())
    }
}

impl YcsbPowerTime {
    fn ycsb_time(&self, data_path: PathBuf, plot_path: &Path, info: &BenchmarkInfo) -> Result<()> {
        let config = info.args.downcast_ref::<Ycsb>().unwrap();
        if config._ycsb_op_type.as_ref().unwrap() != &OpType::Run {
            println!("Skipping: {:?}", config);
            return Ok(());
        }
        let name = format!("{}-ps{}-{:?}", info.name, info.power_state, config.fs);

        let trace_file = data_path.join("trace.out");
        if trace_file.exists() {
            let trace = parse_trace(&std::fs::read_to_string(&trace_file)?, &config.fs)?;
            write_csv(
                &plot_path.join("plot_data").join(format!("{name}.csv")),
                &trace,
            )?;
        }

        let mut args = vec![
            "plots/ycsb_time.py",
            "--plot_dir",
            plot_path.to_str().unwrap(),
            "--results_dir",
            data_path.to_str().unwrap(),
            "--name",
            &name,
        ]
        .into_iter()
        .map(|x| x.to_owned())
        .collect::<Vec<_>>();
        debug!("{}", args.join(" "));
        if let Some(offset) = &self.offset {
            args.push("--offset".to_owned());
            args.push(offset.to_string());
        }
        let mut child = std::process::Command::new("python3").args(args).spawn()?;
        child.wait()?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SectionedCalculation {
    pub overall: SectionStats,
    pub benchmark: SectionStats,
    pub unmount: SectionStats,
}
