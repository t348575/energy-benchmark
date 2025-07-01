#[cxx::bridge]
mod ffi {
    #[repr(u8)]
    enum SensorType {
        RAPL,
    }

    unsafe extern "C++" {
        include!("pmt.h");
        include!("pmt/src/wrapper/wrapper.hpp");

        type PMT;
        type State;
        fn create(sensor: SensorType) -> Result<UniquePtr<PMT>>;
        fn read(device: Pin<&mut PMT>) -> Result<UniquePtr<State>>;
        fn watts(start: &State, end: &State, pair_id: i32) -> f64;
        fn get_sensor_name(device: Pin<&mut PMT>, sensor_id: i32) -> Result<String>;
    }
}

use std::{
    cmp::min,
    sync::Arc,
    time::{Duration, Instant},
};

use common::{
    sensor::{Sensor, SensorArgs, SensorReply, SensorRequest},
    util::sensor_reader,
};
use cxx::UniquePtr;
use eyre::{ContextCompat, Result};
use flume::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{spawn, sync::Mutex, task::JoinHandle};
use tracing::{debug, error};

#[derive(Error, Debug)]
pub enum PmtError {
    #[error("Creation failed: {0}")]
    CreationFailed(String),
    #[error("Sensor name error: {0}")]
    SensorNameError(String),
    #[error("Invalid sensor type")]
    InvalidSensorType,
    #[error("Measurement failed: {0}")]
    MeasurementError(String),
}

struct InternalPmt(UniquePtr<ffi::PMT>);

unsafe impl Send for InternalPmt {}
unsafe impl Send for ffi::State {}

#[derive(Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub enum SensorType {
    #[default]
    None,
    RAPL,
}

impl TryFrom<SensorType> for ffi::SensorType {
    type Error = PmtError;
    fn try_from(value: SensorType) -> Result<Self, Self::Error> {
        match value {
            SensorType::None => Err(PmtError::InvalidSensorType),
            SensorType::RAPL => Ok(ffi::SensorType::RAPL),
        }
    }
}

impl InternalPmt {
    fn new(sensor: SensorType) -> Result<Self, PmtError> {
        ffi::create(sensor.try_into()?)
            .map_err(|e| PmtError::CreationFailed(e.to_string()))
            .map(Self)
    }

    fn get_sensor_name(&mut self, sensor_id: i32) -> Result<String, PmtError> {
        ffi::get_sensor_name(self.0.pin_mut(), sensor_id)
            .map_err(|e| PmtError::SensorNameError(e.to_string()))
    }

    fn read(&mut self) -> Result<UniquePtr<ffi::State>, PmtError> {
        ffi::read(self.0.pin_mut()).map_err(|e| PmtError::MeasurementError(e.to_string()))
    }

    fn watts(&self, start: &ffi::State, end: &ffi::State, pair_id: i32) -> f64 {
        ffi::watts(start, end, pair_id)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct PmtConfig {
    pub sensor: SensorType,
    pub indexes: Vec<i32>,
}

#[typetag::serde]
impl SensorArgs for PmtConfig {
    fn name(&self) -> &'static str {
        "Pmt"
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Pmt;

impl Sensor for Pmt {
    fn name(&self) -> &'static str {
        "Pmt"
    }

    fn start(
        &self,
        args: &dyn common::sensor::SensorArgs,
        rx: Receiver<SensorRequest>,
        tx: Sender<SensorReply>,
    ) -> Result<JoinHandle<Result<()>>> {
        let args = args
            .downcast_ref::<PmtConfig>()
            .context("Invalid sensor args, expected args for Pmt")?;

        let args = args.clone();
        let handle = spawn(async move {
            if let Err(err) = sensor_reader(
                rx,
                tx,
                &format!("pmt-{:?}", args.sensor),
                args,
                init_pmt,
                |args: &PmtConfig, sensor: &Arc<Mutex<InternalPmt>>, _, last_time| -> std::pin::Pin<Box<dyn Future<Output = Result<Vec<f64>>> + Send>> {
                    Box::pin(read_pmt(args.clone(), sensor.clone(), last_time))
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

async fn init_pmt(args: PmtConfig) -> Result<(Arc<Mutex<InternalPmt>>, Vec<String>)> {
    let mut sensor = InternalPmt::new(args.sensor.clone())?;
    let mut sensor_names = Vec::new();
    for (data_idx, idx) in args.indexes.iter().enumerate() {
        if *idx == -1 {
            sensor_names.push("Total".to_owned());
        } else {
            let name = sensor.get_sensor_name(*idx)?;
            if args.sensor == SensorType::RAPL
                && !sensor_names.is_empty()
                && data_idx - 1 == *idx as usize
                && sensor_names.last().unwrap().starts_with("package-")
                && name.eq("dram")
            {
                let num = format!("{}", sensor_names.last().unwrap().chars().last().unwrap())
                    .parse::<u32>()
                    .unwrap();
                sensor_names.push(format!("dram-{num}"));
                continue;
            }
            sensor_names.push(name);
        }
    }
    debug!(
        "PMT sensor with {:?} initialized with sensors {:?}",
        args.sensor, sensor_names
    );
    Ok((Arc::new(Mutex::new(sensor)), sensor_names))
}

async fn read_pmt(
    args: PmtConfig,
    sensor: Arc<Mutex<InternalPmt>>,
    last_time: Instant,
) -> Result<Vec<f64>> {
    let start_time = Instant::now();
    let mut sensor = sensor.lock().await;
    let start = sensor.read()?;
    async_io::Timer::after(Duration::from_micros(min(
        1000 - start_time.elapsed().as_micros() as u64
            - (last_time.elapsed().as_micros() as u64).saturating_sub(1000),
        1000,
    )))
    .await;
    let end = sensor.read()?;
    let readings = args
        .indexes
        .iter()
        .map(|sensor_idx| sensor.watts(&start, &end, *sensor_idx))
        .collect();
    Ok(readings)
}
