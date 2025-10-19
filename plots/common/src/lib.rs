use std::path::PathBuf;

use common::{
    bench::BenchInfo,
    util::{TimeSeriesAxis, TimeSeriesPlot, TimeSeriesSpec},
};
use default_benches::BenchKind;
use default_sensors::SensorKind;

pub fn default_timeseries_plot(
    kind: BenchKind,
    plot_path: PathBuf,
    data_path: PathBuf,
    name: String,
    bench_info: &BenchInfo,
) -> TimeSeriesSpec {
    TimeSeriesSpec::new(
        kind.name(),
        plot_path.to_path_buf(),
        data_path,
        &name,
        vec![
            TimeSeriesPlot::new(
                None,
                format!("{name}-ssd"),
                "SSD Power",
                TimeSeriesAxis::sensor_time(SensorKind::Powersensor3.filename()),
                powersensor_3::POWERSENSOR_PLOT_AXIS.to_vec(),
            )
            .with_secondary(diskstat::DISKSTAT_PLOT_AXIS.to_vec()),
            TimeSeriesPlot::new(
                None,
                format!("{name}-cpu"),
                "CPU Power",
                TimeSeriesAxis::sensor_time(SensorKind::Rapl.filename()),
                rapl::RAPL_PLOT_AXIS.to_vec(),
            )
            .with_secondary(diskstat::DISKSTAT_PLOT_AXIS.to_vec()),
            TimeSeriesPlot::new(
                None,
                format!("{name}-cpu-freq"),
                "System Info freq.",
                TimeSeriesAxis::sensor_time(SensorKind::Sysinfo.filename()),
                sysinfo::sysinfo_freq_plot_axis(&bench_info.cpu_topology),
            )
            .with_secondary(diskstat::DISKSTAT_PLOT_AXIS.to_vec()),
            TimeSeriesPlot::new(
                None,
                format!("{name}-cpu-load"),
                "System Info load",
                TimeSeriesAxis::sensor_time(SensorKind::Sysinfo.filename()),
                sysinfo::sysinfo_load_plot_axis(&bench_info.cpu_topology),
            )
            .with_secondary(diskstat::DISKSTAT_PLOT_AXIS.to_vec()),
        ],
    )
}
