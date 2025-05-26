use std::{
    cmp::min,
    collections::HashSet,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use common::{
    sensor::{Sensor, SensorArgs, SensorReply, SensorRequest},
    util::sensor_reader,
};
use eyre::{ContextCompat, Result, bail};
use flume::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    spawn,
    sync::Mutex,
    task::JoinHandle,
};
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
    async fn new() -> Result<Self, RaplError> {
        let mut package_ids = HashSet::new();
        let mut dir = tokio::fs::read_dir("/sys/devices/system/cpu/").await?;
        while let Some(entry) = dir.next_entry().await? {
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();

            if !file_name_str.starts_with("cpu") {
                continue;
            }
            let cpu_index_str = &file_name_str[3..];
            if cpu_index_str.is_empty() || !cpu_index_str.chars().all(|c| c.is_digit(10)) {
                continue;
            }

            let topology_path = entry.path().join("topology/physical_package_id");
            if Path::new(&topology_path).exists() {
                let mut file = File::open(topology_path).await?;
                let mut result = String::new();
                file.read_to_string(&mut result).await?;
                if let Ok(package_id) = result.trim().parse::<u32>() {
                    package_ids.insert(package_id);
                }
            }
        }

        let mut packages = package_ids.into_iter().collect::<Vec<_>>();
        packages.sort();
        debug!("cpu packages: {packages:?}");
        let mut files = Vec::new();
        for package in &packages {
            debug!("/sys/class/powercap/intel-rapl:{package}/energy_uj");
            debug!("/sys/class/powercap/intel-rapl:{package}:0/energy_uj");
            let cpu = File::open(format!(
                "/sys/class/powercap/intel-rapl:{package}/energy_uj"
            ))
            .await?;
            let dram = File::open(format!(
                "/sys/class/powercap/intel-rapl:{package}:0/energy_uj"
            ))
            .await?;
            files.push((cpu, dram));
        }

        Ok(Self { packages, files })
    }

    async fn read(&mut self, results: &mut [(u64, u64)]) -> Result<()> {
        for (idx, (cpu, dram)) in self.files.iter_mut().enumerate() {
            let mut data = String::new();
            cpu.seek(std::io::SeekFrom::Start(0)).await?;
            cpu.read_to_string(&mut data).await?;
            let cpu = data.trim().parse::<u64>()?;
            data = String::new();
            dram.seek(std::io::SeekFrom::Start(0)).await?;
            dram.read_to_string(&mut data).await?;
            let dram = data.trim().parse::<u64>()?;
            results[idx] = (cpu, dram);
        }
        Ok(())
    }

    fn size(&self) -> usize {
        self.files.len()
    }

    fn watts(start: u64, end: u64, elapsed: u64) -> f64 {
        let entry = (end - start) as f64 / 1e6;
        entry / (elapsed as f64 / 1e6)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct RaplConfig;

#[typetag::serde]
impl SensorArgs for RaplConfig {
    fn name(&self) -> &'static str {
        "Rapl"
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Rapl;

impl Sensor for Rapl {
    fn name(&self) -> &'static str {
        "Rapl"
    }

    fn start(
        &self,
        args: &dyn common::sensor::SensorArgs,
        rx: Receiver<SensorRequest>,
        tx: Sender<SensorReply>,
    ) -> Result<JoinHandle<Result<()>>> {
        let args = args
            .downcast_ref::<RaplConfig>()
            .context("Invalid sensor args, expected args for Rapl")?;

        let args = args.clone();
        let handle = spawn(async move {
            if let Err(err) = sensor_reader(
                rx,
                tx,
                "rapl",
                args,
                init_rapl,
                |args: &RaplConfig, sensor: &Arc<Mutex<InternalRapl>>, _, last_time| -> std::pin::Pin<Box<dyn Future<Output = Result<Vec<f64>>> + Send>> {
                    Box::pin(read_rapl(args.clone(), sensor.clone(), last_time))
                },
            )
            .await
            {
                error!("{err:#?}");
                return Err(err);
            }
            Ok(())
        });
        Ok(handle)
    }
}

async fn init_rapl(_: RaplConfig) -> Result<(Arc<Mutex<InternalRapl>>, Vec<String>)> {
    let sensor = InternalRapl::new().await?;
    let mut sensor_names = sensor
        .packages
        .iter()
        .map(|x| [format!("package-{}", x), format!("dram-{}", x)])
        .flatten()
        .collect::<Vec<_>>();
    sensor_names.insert(0, "Total".to_owned());
    debug!("RAPL sensor initialized!");
    Ok((Arc::new(Mutex::new(sensor)), sensor_names))
}

async fn read_rapl(
    _: RaplConfig,
    sensor: Arc<Mutex<InternalRapl>>,
    last_time: Instant,
) -> Result<Vec<f64>> {
    let start_time = Instant::now();
    let mut sensor = sensor.lock().await;
    let mut start = vec![(0, 0); sensor.size()];
    let mut end = vec![(0, 0); sensor.size()];
    let sensor_read_time = Instant::now();
    sensor.read(&mut start).await?;
    async_io::Timer::after(Duration::from_micros(min(
        1000 - start_time.elapsed().as_micros() as u64
            - (last_time.elapsed().as_micros() as u64).saturating_sub(1000),
        1000,
    )))
    .await;
    let sensor_end_time = sensor_read_time.elapsed().as_micros() as u64;
    sensor.read(&mut end).await?;

    let no_changes = start.iter().zip(end.iter()).any(|(x, y)| x.0 >= y.0);
    if no_changes {
        bail!("No power changes");
    }

    let mut readings = start
        .into_iter()
        .zip(end)
        .map(|(start, end)| {
            [
                InternalRapl::watts(start.0, end.0, sensor_end_time),
                InternalRapl::watts(start.1, end.1, sensor_end_time),
            ]
        })
        .flatten()
        .collect::<Vec<_>>();
    readings.insert(0, readings.iter().sum());
    Ok(readings)
}
