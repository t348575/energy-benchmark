use std::{
    collections::{HashMap, HashSet},
    fs::write,
    path::{Path, PathBuf},
};

use common::{
    MB_TO_MIB,
    bench::BenchmarkInfo,
    config::{Config, Settings},
    plot::{Plot, PlotType},
    util::{SectionStats, calculate_sectioned, plot_python, power_energy_calculator},
};
use eyre::{Context, ContextCompat, Result};
use futures::future::join_all;
use itertools::Itertools;
use mlperf::{
    Mlperf,
    result::{MlperfMetrics, find_summary},
};
use rayon::iter::{IntoParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use tokio::fs::{create_dir_all, read_to_string};
use tracing::debug;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MlperfBasic;

#[derive(Debug, Clone)]
struct PlotEntry {
    result: MlperfMetrics,
    info: BenchmarkInfo,
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

        async fn read_results_json(folder: PathBuf) -> Result<MlperfMetrics> {
            let summary_file_path =
                find_summary(&folder.join("training")).context("Could not find summary.json")?;
            let data = read_to_string(summary_file_path).await?;
            serde_json::from_str(&data).context("Parse summary.json")
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
                let (json, powersensor3, rapl, _dir, info) = item;
                let rapl = rapl.context("Read rapl").unwrap();
                let powersensor3 = powersensor3.context("Read powersensor3").unwrap();

                let (_, rapl_overall, _) = calculate_sectioned::<_, 0>(
                    None,
                    &rapl,
                    &["Total"],
                    &[(0.0, 200.0)],
                    power_energy_calculator,
                    None,
                )
                .context("Calculate rapl means")
                .unwrap();
                let (_, ps3_overall, _times) = calculate_sectioned::<_, 0>(
                    None,
                    &powersensor3,
                    &["Total"],
                    &[(0.0, 8.5)],
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
        let dirs = [&power_dir, &throughput_dir, &efficiency_dir];
        for dir in join_all(dirs.iter().map(create_dir_all)).await.into_iter() {
            dir?;
        }

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
            .map(|x| self.bar_plot(x.0, x.1, x.2, x.3, x.4, x.5))
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
                "Samples/J",
                "overall",
                false,
            ),
            (
                plot_path.join(format!("{}-bytes-j+cpu.pdf", &experiment_name)),
                bytes_j,
                "MiB/J",
                "overall",
                false,
            ),
            (
                plot_path.join(format!("{}-bytes-j.pdf", &experiment_name)),
                bytes_j_ssd,
                "MiB/J",
                "overall",
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

        let dir = plot_path.join("mlperf_time");
        let inner_dir = dir.join(&entries[0].1.name);
        create_dir_all(&inner_dir).await?;
        create_dir_all(inner_dir.join("plot_data")).await?;
        let results = entries
            .into_iter()
            .map(|data| self.mlperf_time(data_path.join(data.0.clone()), &inner_dir, &data.1))
            .collect::<Vec<_>>();
        for item in results {
            item?;
        }
        Ok(())
    }
}

impl MlperfPowerTime {
    fn mlperf_time(
        &self,
        data_path: PathBuf,
        plot_path: &Path,
        info: &BenchmarkInfo,
    ) -> Result<()> {
        let config = info.args.downcast_ref::<Mlperf>().unwrap();
        let name = format!(
            "{}-ps{}-{}",
            info.name, info.power_state, config.n_accelerators[0]
        );

        let mut args = vec![
            ("--plot_dir", plot_path.to_str().unwrap()),
            ("--results_dir", data_path.to_str().unwrap()),
            ("--name", &name),
        ]
        .into_iter()
        .map(|x| (x.0.to_owned(), x.1.to_owned()))
        .collect::<Vec<_>>();
        if let Some(offset) = &self.offset {
            args.push(("--offset".to_owned(), offset.to_string()));
        }
        plot_python("mlperf_time", &args)?;
        Ok(())
    }
}
