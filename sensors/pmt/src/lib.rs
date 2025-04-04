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

use std::{sync::Arc, time::Duration};

use common::{
    sensor::{Sensor, SensorArgs, SensorReply, SensorRequest},
    util::simple_sensor_reader,
};
use cxx::UniquePtr;
use eyre::{ContextCompat, Result};
use flume::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{spawn, sync::Mutex, task::JoinHandle, time::sleep};
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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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
            .map(|inner| Self(inner))
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
            if let Err(err) = simple_sensor_reader(
                rx,
                tx,
                &format!("pmt-{:?}", args.sensor),
                args,
                init_pmt,
                |args: &PmtConfig, sensor: &Arc<Mutex<InternalPmt>>, _| -> std::pin::Pin<Box<dyn Future<Output = Result<Vec<f64>>> + Send>> {
                    Box::pin(read_pmt(args.clone(), sensor.clone()))
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

fn init_pmt(args: &PmtConfig) -> Result<(Arc<Mutex<InternalPmt>>, Vec<String>)> {
    let mut sensor = InternalPmt::new(args.sensor.clone())?;
    let mut sensor_names = Vec::new();
    for idx in &args.indexes {
        if *idx == -1 {
            sensor_names.push("Total".to_owned());
        } else {
            sensor_names.push(sensor.get_sensor_name(*idx)?);
        }
    }
    debug!("PMT sensor with {:?} initialized", args.sensor);
    Ok((Arc::new(Mutex::new(sensor)), sensor_names))
}

async fn read_pmt(args: PmtConfig, sensor: Arc<Mutex<InternalPmt>>) -> Result<Vec<f64>> {
    let mut sensor = sensor.lock().await;
    let start = sensor.read()?;
    sleep(Duration::from_millis(1)).await;
    let end = sensor.read()?;
    let readings = args
        .indexes
        .iter()
        .map(|sensor_idx| sensor.watts(&start, &end, *sensor_idx))
        .collect();
    Ok(readings)
}
