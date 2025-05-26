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
        Filesystem, SectionStats, calculate_sectioned, parse_data_size, parse_trace, plot_python,
        power_energy_calculator, write_csv,
    },
};
use eyre::{Context, ContextCompat, Result};
use filebench::{Filebench, result::FilebenchSummary};
use futures::future::join_all;
use itertools::Itertools;
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use tokio::fs::{create_dir_all, read_to_string};
use tracing::debug;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FilebenchBasic {
    var_name: String,
}

#[derive(Debug, Clone)]
struct PlotEntry {
    result: FilebenchSummary,
    info: BenchmarkInfo,
    args: Filebench,
    ssd_power: SectionedCalculation,
    cpu_power: SectionedCalculation,
    times: [usize; 3],
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

        async fn read_results_json(folder: PathBuf) -> Result<FilebenchSummary> {
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

                let (rapl_means, rapl_overall, _) = calculate_sectioned::<_, 3>(
                    &markers,
                    &rapl,
                    "Total",
                    0.0,
                    200.0,
                    power_energy_calculator,
                )
                .context("Calculate rapl means")
                .unwrap();
                let (powersensor3_means, ps3_overall, times) = calculate_sectioned::<_, 3>(
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
                    times,
                }
            })
            .collect::<Vec<_>>();

        let experiment_name = ready_entries[0].info.name.clone();
        let throughput_dir = plot_path.join("throughput");
        create_dir_all(&throughput_dir).await?;
        self.bar_plot(
            ready_entries.clone(),
            settings,
            throughput_dir.join(format!("{}-read.pdf", experiment_name)),
            "throughput",
            None,
            "",
            |data| {
                data.result
                    .ops_stats
                    .iter()
                    .filter(|x| x.name.starts_with("readfile"))
                    .map(|x| x.mb_per_sec)
                    .sum()
            },
        )?;

        self.bar_plot(
            ready_entries.clone(),
            settings,
            throughput_dir.join(format!("{}-write.pdf", experiment_name)),
            "throughput",
            None,
            "",
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
        )?;

        let iops_dir = plot_path.join("iops");
        create_dir_all(&iops_dir).await?;
        self.bar_plot(
            ready_entries.clone(),
            settings,
            iops_dir.join(format!("{}.pdf", experiment_name)),
            "throughput",
            None,
            "kOPS/s",
            |data| data.result.summary.ops_per_sec / 1000.0,
        )?;

        let latency_dir = plot_path.join("latency");
        self.bar_plot(
            ready_entries.clone(),
            settings,
            latency_dir.join(format!("{}.pdf", experiment_name)),
            "latency",
            None,
            "ms",
            |data| data.result.summary.latency_ms,
        )?;

        let power_dir = plot_path.join("power");
        self.bar_plot(
            ready_entries.clone(),
            settings,
            power_dir.join(format!("{}-overall.pdf", experiment_name)),
            "power",
            Some("CPU + DRAM"),
            "ms",
            |data| data.cpu_power.overall.power.unwrap(),
        )?;
        self.bar_plot(
            ready_entries.clone(),
            settings,
            power_dir.join(format!("{}-init.pdf", experiment_name)),
            "power",
            Some("CPU + DRAM"),
            "ms",
            |data| data.cpu_power.init.power.unwrap(),
        )?;
        self.bar_plot(
            ready_entries.clone(),
            settings,
            power_dir.join(format!("{}-benchmark.pdf", experiment_name)),
            "power",
            Some("CPU + DRAM"),
            "ms",
            |data| data.cpu_power.benchmark.power.unwrap(),
        )?;
        self.bar_plot(
            ready_entries.clone(),
            settings,
            power_dir.join(format!("{}-post-benchmark.pdf", experiment_name)),
            "power",
            Some("CPU + DRAM"),
            "ms",
            |data| data.cpu_power.post_benchmark.power.unwrap(),
        )?;

        let efficiency_dir = plot_path.join("efficiency");
        create_dir_all(&efficiency_dir).await?;
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
        let mut args = vec![
            ("--data", plot_data_file.to_str().unwrap()),
            ("--filepath", filepath.to_str().unwrap()),
            ("--x_label_name", x_label),
            ("--experiment_name", &experiment_name),
            ("--labels", &labels),
        ];

        if let Some(name) = y_name {
            args.push(("--name", name));
        }

        plot_python(plotting_file, &args)?;
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
        let mut iops_j_init = iops_j_overall.clone();
        let mut iops_j_benchmark = iops_j_overall.clone();
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

                let benchmark_time = (item.times[2] - item.times[1]) as f64 / 1000.0;
                (
                    x,
                    y,
                    iops / ((item.ssd_power.overall.energy.unwrap()
                        + item.cpu_power.overall.energy.unwrap())
                        / benchmark_time),
                    iops / ((item.ssd_power.init.energy.unwrap()
                        + item.cpu_power.init.energy.unwrap())
                        / benchmark_time),
                    iops / ((item.ssd_power.benchmark.energy.unwrap()
                        + item.cpu_power.benchmark.energy.unwrap())
                        / benchmark_time),
                    iops / ((item.ssd_power.post_benchmark.energy.unwrap()
                        + item.cpu_power.post_benchmark.energy.unwrap())
                        / benchmark_time),
                    bytes
                        / ((item.ssd_power.overall.energy.unwrap()
                            + item.cpu_power.overall.energy.unwrap())
                            / benchmark_time),
                    bytes
                        / ((item.ssd_power.init.energy.unwrap()
                            + item.cpu_power.init.energy.unwrap())
                            / benchmark_time),
                    bytes
                        / ((item.ssd_power.benchmark.energy.unwrap()
                            + item.cpu_power.benchmark.energy.unwrap())
                            / benchmark_time),
                    bytes
                        / ((item.ssd_power.post_benchmark.energy.unwrap()
                            + item.cpu_power.post_benchmark.energy.unwrap())
                            / benchmark_time),
                    item.ssd_power.benchmark.power.unwrap() * latency,
                )
            })
            .collect::<Vec<_>>();
        for item in results {
            let x = item.0;
            let y = item.1;
            iops_j_overall[x][y] = item.2;
            iops_j_init[x][y] = item.3;
            iops_j_benchmark[x][y] = item.4;
            iops_j_post_benchmark[x][y] = item.5;
            bytes_j_overall[x][y] = item.6;
            bytes_j_init[x][y] = item.7;
            bytes_j_benchmark[x][y] = item.8;
            bytes_j_post_benchmark[x][y] = item.9;
            edp[x][y] = item.10;
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
                plot_path.join(format!("{}-iops-j-init.pdf", &experiment_name)),
                iops_j_init,
                "IOPS/J",
                "init",
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
                plot_path.join(format!("{}-iops-j-post-benchmark.pdf", &experiment_name)),
                iops_j_post_benchmark,
                "IOPS/J",
                "post-benchmark",
                false,
            ),
            (
                plot_path.join(format!("{}-bytes-j-overall.pdf", &experiment_name)),
                bytes_j_overall,
                "Bytes/J",
                "overall",
                false,
            ),
            (
                plot_path.join(format!("{}-bytes-j-init.pdf", &experiment_name)),
                bytes_j_init,
                "Bytes/J",
                "init",
                false,
            ),
            (
                plot_path.join(format!("{}-bytes-j-benchmark.pdf", &experiment_name)),
                bytes_j_benchmark,
                "Bytes/J",
                "benchmark",
                false,
            ),
            (
                plot_path.join(format!("{}-bytes-j-post-benchmark.pdf", &experiment_name)),
                bytes_j_post_benchmark,
                "Bytes/J",
                "post-benchmark",
                false,
            ),
            (
                plot_path.join(format!("{}-edp.pdf", &experiment_name)),
                edp,
                "EDP",
                "edp",
                true,
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

        let dir = plot_path.join("filebench_time");
        let inner_dir = dir.join(&entries[0].1.name);
        create_dir_all(&inner_dir).await?;
        create_dir_all(inner_dir.join("plot_data")).await?;
        let results = entries
            .into_par_iter()
            .map(|data| self.filebench_time(data_path.join(data.0.clone()), &inner_dir, &data.1))
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
        info: &BenchmarkInfo,
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
            let trace = parse_trace(&std::fs::read_to_string(&trace_file)?, fs)?;
            write_csv(
                &plot_path.join("plot_data").join(format!("{name}.csv")),
                &trace,
            )?;
        }

        let mut args = vec![
            "plots/filebench_time.py",
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
    pub init: SectionStats,
    pub benchmark: SectionStats,
    pub post_benchmark: SectionStats,
}
