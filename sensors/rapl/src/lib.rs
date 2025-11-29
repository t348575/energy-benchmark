use std::{
    fs::File,
    os::unix::fs::FileExt,
    sync::LazyLock,
    time::{Duration, Instant},
};

use common::{
    config::Settings,
    sensor::{Sensor, SensorArgs, SensorReply, SensorRequest},
    util::{SensorError, TimeSeriesAxis, blocking_sensor_reader, get_cpu_topology},
};
use eyre::{ContextCompat, Result};
use flume::{Receiver, Sender};
use sensor_common::SensorKind;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{runtime::Handle, spawn, task::JoinHandle};
use tracing::{debug, error};

#[derive(Error, Debug)]
pub enum RaplError {
    #[error("Creation failed: {0}")]
    CreationFailed(String),
    #[error("Sensor name error: {0}")]
    SensorNameError(String),
    #[error("Measurement failed: {0}")]
    MeasurementError(String),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

struct InternalRapl {
    packages: Vec<u32>,
    files: Vec<(File, File)>,
}

impl InternalRapl {
    fn new() -> Result<Self, RaplError> {
        let topology = Handle::current()
            .block_on(get_cpu_topology())
            .map_err(|e| RaplError::CreationFailed(e.to_string()))?;
        let mut packages = topology.into_iter().map(|x| x.0).collect::<Vec<_>>();
        packages.sort();
        debug!("cpu packages: {packages:?}");

        let mut files = Vec::new();
        for package in &packages {
            debug!("/sys/class/powercap/intel-rapl:{package}/energy_uj");
            debug!("/sys/class/powercap/intel-rapl:{package}:0/energy_uj");
            let cpu = File::open(format!(
                "/sys/class/powercap/intel-rapl:{package}/energy_uj"
            ))
            .map_err(|e| RaplError::CreationFailed(e.to_string()))?;
            let dram = File::open(format!(
                "/sys/class/powercap/intel-rapl:{package}:0/energy_uj"
            ))
            .map_err(|e| RaplError::CreationFailed(e.to_string()))?;
            files.push((cpu, dram));
        }

        Ok(Self { packages, files })
    }

    fn read(&self, result: &mut [(u64, u64)]) {
        let mut buf_cpu = [0u8; 32];
        let mut buf_dram = [0u8; 32];
        self.files.iter().zip(result.iter_mut()).for_each(|(s, r)| {
            use atoi::FromRadix10;
            let cpu = s.0.read_at(&mut buf_cpu, 0).unwrap();
            let dram = s.1.read_at(&mut buf_dram, 0).unwrap();
            let cpu = u64::from_radix_10(&buf_cpu[0..cpu]).0;
            let dram = u64::from_radix_10(&buf_dram[0..dram]).0;
            *r = (cpu, dram)
        });
    }

    fn watts(start: u64, end: u64, elapsed: u64) -> f64 {
        let entry = (end - start) as f64 / 1e6;
        entry / (elapsed as f64 / 1e6)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RaplConfig;

#[typetag::serde]
impl SensorArgs for RaplConfig {
    fn name(&self) -> SensorKind {
        SensorKind::Rapl
    }
}

const RAPL_FILENAME: &str = "rapl.csv";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rapl;

impl Sensor for Rapl {
    fn name(&self) -> SensorKind {
        SensorKind::Rapl
    }

    fn filename(&self) -> &'static str {
        RAPL_FILENAME
    }

    fn start(
        &self,
        args: &dyn common::sensor::SensorArgs,
        _: &Settings,
        rx: Receiver<SensorRequest>,
        tx: Sender<SensorReply>,
    ) -> Result<JoinHandle<Result<()>>> {
        let args = args
            .downcast_ref::<RaplConfig>()
            .context("Invalid sensor args, expected args for Rapl")?;

        let args = args.clone();
        let handle = spawn(async move {
            if let Err(err) = blocking_sensor_reader(
                rx,
                tx,
                RAPL_FILENAME,
                args,
                init_rapl,
                |args, sensor, _, last_time| -> Result<Vec<f64>, SensorError> {
                    read_rapl(args, sensor, last_time)
                },
            ) {
                error!("{err:#?}");
                return Err(err);
            }
            Ok(())
        });
        Ok(handle)
    }
}

fn init_rapl(_: RaplConfig) -> Result<(InternalRapl, Vec<String>)> {
    let sensor = InternalRapl::new()?;
    let mut sensor_names = sensor
        .packages
        .iter()
        .flat_map(|x| [format!("package-{x}"), format!("dram-{x}")])
        .collect::<Vec<_>>();
    sensor_names.insert(0, "Total".to_owned());
    debug!("RAPL sensor initialized!");
    Ok((sensor, sensor_names))
}

fn read_rapl(
    _: &RaplConfig,
    sensor: &mut InternalRapl,
    _: Instant,
) -> Result<Vec<f64>, SensorError> {
    let mut start = vec![(0u64, 0u64); sensor.files.len()];
    let mut end = vec![(0u64, 0u64); sensor.files.len()];
    let sensor_read_time = Instant::now();
    sensor.read(&mut start);
    std::thread::sleep(Duration::from_micros(1000));
    sensor.read(&mut end);
    let sensor_end_time = sensor_read_time.elapsed().as_micros() as u64;

    let no_changes = start.iter().zip(end.iter()).any(|(x, y)| x.0 >= y.0);
    if no_changes {
        return Err(SensorError::NoChanges);
    }

    let mut readings = start
        .iter()
        .zip(&end)
        .flat_map(|(start, end)| {
            [
                InternalRapl::watts(start.0, end.0, sensor_end_time),
                InternalRapl::watts(start.1, end.1, sensor_end_time),
            ]
        })
        .collect::<Vec<_>>();
    readings.insert(0, readings.iter().sum());
    Ok(readings)
}

pub static RAPL_PLOT_AXIS: LazyLock<[TimeSeriesAxis; 1]> = LazyLock::new(|| {
    [TimeSeriesAxis::sensor(
        RAPL_FILENAME,
        "total_smoothed",
        "CPU Power",
        "CPU Power (Watts)",
    )]
});
