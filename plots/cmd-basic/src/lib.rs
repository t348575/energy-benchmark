use std::path::{Path, PathBuf};

use cmd::Cmd;
use common::{
    bench::{BenchInfo, BenchParams},
    config::{Config, Settings},
    plot::{Plot, PlotType, collect_run_groups, ensure_dirs},
    util::{
        BarChartKind, Filesystem, SectionStats, calculate_sectioned, make_power_state_bar_config,
        plot_bar_chart, power_energy_calculator, sysinfo_average_calculator,
    },
};
use eyre::{Context, Result, bail};
use futures::future::join_all;
use plot_common::impl_power_time_plot;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use tokio::fs::read_to_string;
use tracing::debug;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CmdBasic;

#[derive(Debug, Clone)]
struct PlotEntry {
    info: BenchParams,
    ssd_power: SectionStats,
    cpu_power: SectionStats,
    system_power: SectionStats,
    freq: f64,
    load: f64,
}

#[async_trait::async_trait]
#[typetag::serde]
impl Plot for CmdBasic {
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
                    read_to_string(run_dir.join("powersensor3.csv")).await,
                    read_to_string(run_dir.join("rapl.csv")).await,
                    read_to_string(run_dir.join("netio-http.csv")).await,
                    read_to_string(run_dir.join("sysinfo.csv")).await,
                    dir,
                    info,
                )
            }
        }))
        .await;
        let ready_entries = entries
            .into_par_iter()
            .map(|item| {
                let (powersensor3, rapl, system, sysinfo, _, info) = item;
                let rapl = rapl.context("Read rapl").unwrap();
                let powersensor3 = powersensor3.context("Read powersensor3").unwrap();
                let sysinfo = sysinfo.context("Read sysinfo").unwrap();
                let system = system.context("Read system power").unwrap();

                let (_, rapl_overall, _) = calculate_sectioned::<_, 0>(
                    None,
                    &rapl,
                    &["Total"],
                    &[(0.0, settings.cpu_max_power_watts)],
                    power_energy_calculator,
                )
                .context("Calculate rapl means")
                .unwrap();
                let (_, ps3_overall, _times) = calculate_sectioned::<_, 0>(
                    None,
                    &powersensor3,
                    &["Total"],
                    &[(0.0, bench_info.device_power_states[0].0)],
                    power_energy_calculator,
                )
                .context("Calculate powersensor3 means")
                .unwrap();

                let (_, system, _) = calculate_sectioned::<_, 0>(
                    None,
                    &system,
                    &[r#"load-\S+"#],
                    &[(0.0, settings.cpu_max_power_watts * 2.0)],
                    power_energy_calculator,
                )
                .context("Calculate system power means")
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
                )
                .context("Calculate sysinfo means")
                .unwrap();

                PlotEntry {
                    info,
                    ssd_power: ps3_overall,
                    cpu_power: rapl_overall,
                    system_power: system,
                    freq,
                    load,
                }
            })
            .collect::<Vec<_>>();

        let experiment_name = ready_entries[0].info.name.clone();
        let power_dir = plot_path.join("power");
        ensure_dirs(std::slice::from_ref(&power_dir)).await?;

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
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-server.pdf")),
                "power",
                "%",
                |data| data.system_power.power.unwrap(),
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-freq.pdf")),
                "freq",
                "CPU",
                |data| data.freq,
            ),
            (
                ready_entries.clone(),
                settings,
                power_dir.join(format!("{experiment_name}-load.pdf")),
                "load",
                "Linux",
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

        Ok(())
    }
}

impl CmdBasic {
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

        let results = results
            .iter()
            .map(|x| x.iter().map(|x| x.1).collect::<Vec<_>>())
            .collect::<Vec<_>>();

        let chart_kind = match plotting_file {
            "throughput" => BarChartKind::Throughput,
            "power" => BarChartKind::Power,
            "freq" => BarChartKind::Freq,
            "load" => BarChartKind::Load,
            other => bail!("Unsupported plotting file {other}"),
        };
        let config = make_power_state_bar_config(chart_kind, x_label, &experiment_name, None);
        plot_bar_chart(
            &filepath,
            results,
            vec!["Result".to_owned()],
            config,
            bench_info,
        )
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CmdPowerTime {
    offset: Option<usize>,
}
impl_power_time_plot!(CmdPowerTime, Cmd, |_: &Cmd| "0", |_: &Cmd| Filesystem::Ext4);
