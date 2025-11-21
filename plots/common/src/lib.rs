use std::path::PathBuf;

use common::{
    bench::BenchInfo,
    sensor::Sensor,
    util::{TimeSeriesAxis, TimeSeriesPlot, TimeSeriesSpec},
};
use default_benches::BenchKind;
use macros::if_sensor;
use sensor_common::SensorKind;

pub fn default_timeseries_plot(
    kind: BenchKind,
    plot_path: PathBuf,
    data_path: PathBuf,
    name: String,
    #[allow(unused_variables)] bench_info: &BenchInfo,
) -> TimeSeriesSpec {
    let mut plots = Vec::new();
    let diskstat = if_sensor!(
        "Diskstat",
        diskstat::DISKSTAT_PLOT_AXIS.to_vec(),
        Vec::new()
    );

    if let Some(ps3) = SensorKind::get("Powersensor3")
        && !diskstat.is_empty()
    {
        plots.push(
            TimeSeriesPlot::new(
                None,
                format!("{name}-ssd"),
                "SSD Power",
                TimeSeriesAxis::sensor_time(get_sensor(ps3).filename()),
                if_sensor!(
                    "Powersensor3",
                    powersensor3::POWERSENSOR_PLOT_AXIS.to_vec(),
                    Vec::new()
                ),
            )
            .with_secondary(diskstat.clone()),
        );
    }

    if let Some(rapl) = SensorKind::get("Rapl")
        && !diskstat.is_empty()
    {
        plots.push(
            TimeSeriesPlot::new(
                None,
                format!("{name}-cpu"),
                "CPU Power",
                TimeSeriesAxis::sensor_time(get_sensor(rapl).filename()),
                if_sensor!("Rapl", rapl::RAPL_PLOT_AXIS.to_vec(), Vec::new()),
            )
            .with_secondary(diskstat.clone()),
        );
    }

    if let Some(sysinfo) = SensorKind::get("Sysinfo")
        && !diskstat.is_empty()
    {
        plots.push(
            TimeSeriesPlot::new(
                None,
                format!("{name}-cpu-freq"),
                "System Info freq.",
                TimeSeriesAxis::sensor_time(get_sensor(sysinfo).filename()),
                if_sensor!(
                    "Sysinfo",
                    sysinfo::sysinfo_freq_plot_axis(&bench_info.cpu_topology),
                    Vec::new()
                ),
            )
            .with_secondary(diskstat.clone()),
        );
    }

    if let Some(sysinfo) = SensorKind::get("Sysinfo")
        && !diskstat.is_empty()
    {
        plots.push(
            TimeSeriesPlot::new(
                None,
                format!("{name}-cpu-load"),
                "System Info load",
                TimeSeriesAxis::sensor_time(get_sensor(sysinfo).filename()),
                if_sensor!(
                    "Sysinfo",
                    sysinfo::sysinfo_load_plot_axis(&bench_info.cpu_topology),
                    Vec::new()
                ),
            )
            .with_secondary(diskstat),
        );
    }

    TimeSeriesSpec::new(
        kind.name(),
        plot_path.to_path_buf(),
        data_path,
        &name,
        plots,
    )
}

fn get_sensor(kind: SensorKind) -> &'static dyn Sensor {
    default_sensors::SENSORS
        .get()
        .unwrap()
        .iter()
        .find(|x| x.name() == kind)
        .unwrap()
        .as_ref()
}

#[macro_export]
macro_rules! impl_power_time_plot {
    ($struct_ty:ident, $bench:ident, $dir_accessor:expr, $fs_accessor:expr) => {
        #[async_trait::async_trait]
        #[typetag::serde]
        impl Plot for $struct_ty {
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

                let slug = stringify!($bench).to_lowercase();
                let dir = plot_path.join(format!("{}_time", slug));
                let inner_dir = dir.join(&groups[0].info.name);
                let dir_list = vec![dir.clone(), inner_dir.clone(), inner_dir.join("plot_data")];
                common::plot::ensure_dirs(&dir_list).await?;

                for group in &groups {
                    self.__power_time_impl(
                        data_path.join(&group.dir),
                        &inner_dir,
                        &group.info,
                        info,
                    )?;
                }
                Ok(())
            }
        }

        impl $struct_ty {
            fn __power_time_impl(
                &self,
                data_path: std::path::PathBuf,
                plot_path: &Path,
                info: &BenchParams,
                bench_info: &BenchInfo,
            ) -> Result<()> {
                let config = info.args.downcast_ref::<$bench>().unwrap();
                let extra = ($dir_accessor)(config);
                let name = format!("{}-ps{}-{}", info.name, info.power_state, extra);

                let trace_file = data_path.join("trace.out");
                if trace_file.exists() {
                    let fs = ($fs_accessor)(config);
                    let trace = common::util::parse_trace(&std::fs::File::open(&trace_file)?, &fs)?;
                    common::util::write_csv(
                        &plot_path.join("plot_data").join(format!("{name}.csv")),
                        &trace,
                    )?;
                }

                common::util::plot_time_series(
                    plot_common::default_timeseries_plot(
                        default_benches::BenchKind::$bench,
                        plot_path.to_path_buf(),
                        data_path,
                        name,
                        bench_info,
                    )
                    .with_offset(self.offset.unwrap_or(0)),
                )?;
                Ok(())
            }
        }
    };
}
