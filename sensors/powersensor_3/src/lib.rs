#[cxx::bridge(namespace = "powersensor3_rs")]
mod ffi {
    unsafe extern "C++" {
        include!("PowerSensor.hpp");
        include!("powersensor_3/src/wrapper/wrapper.hpp");

        type PowerSensor;
        type State;

        fn create(device: &str) -> Result<UniquePtr<PowerSensor>>;
        fn read(sensor: &PowerSensor) -> Result<UniquePtr<State>>;
        fn calculate_watts(start: &State, end: &State, pair_id: i32) -> f64;
        fn get_sensor_name(sensor: &PowerSensor, sensor_id: i32) -> Result<String>;
    }
}

use std::{
    cmp::min,
    sync::Arc,
    time::{Duration, Instant},
};

use common::{
    sensor::{Sensor, SensorArgs, SensorReply, SensorRequest},
    util::{SensorError, sensor_reader},
};
use cxx::UniquePtr;
use eyre::{Context, ContextCompat, Result};
use flume::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{spawn, sync::Mutex, task::JoinHandle};
use tracing::{debug, error};

#[derive(Error, Debug)]
pub enum PowerSensorError {
    #[error("Failed to create sensor: {0}")]
    CreationFailed(String),
    #[error("Measurement error: {0}")]
    MeasurementError(String),
    #[error("Failed to get sensor name: {0}")]
    SensorNameError(String),
}

struct InternalPowersensor3(UniquePtr<ffi::PowerSensor>);

unsafe impl Send for InternalPowersensor3 {}
unsafe impl Sync for InternalPowersensor3 {}

impl InternalPowersensor3 {
    fn new(device: &str) -> Result<Self, PowerSensorError> {
        ffi::create(device)
            .map_err(|e| PowerSensorError::CreationFailed(e.to_string()))
            .map(Self)
    }

    fn read(&self) -> Result<SensorState, PowerSensorError> {
        ffi::read(&self.0)
            .map(SensorState)
            .map_err(|e| PowerSensorError::MeasurementError(e.to_string()))
    }

    fn get_sensor_name(&self, sensor_id: i32) -> Result<String, PowerSensorError> {
        ffi::get_sensor_name(&self.0, sensor_id)
            .map_err(|e| PowerSensorError::SensorNameError(e.to_string()))
    }
}

struct SensorState(UniquePtr<ffi::State>);

unsafe impl Send for SensorState {}

impl SensorState {
    fn watts(&self, other: &SensorState, pair_id: Option<i32>) -> f64 {
        ffi::calculate_watts(&self.0, &other.0, pair_id.unwrap_or(-1))
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Powersensor3Config {
    pub device: String,
    pub indexes: Vec<i32>,
}

#[typetag::serde]
impl SensorArgs for Powersensor3Config {
    fn name(&self) -> &'static str {
        "Powersensor3"
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Powersensor3;

impl Sensor for Powersensor3 {
    fn name(&self) -> &'static str {
        "Powersensor3"
    }

    fn start(
        &self,
        args: &dyn SensorArgs,
        rx: Receiver<SensorRequest>,
        tx: Sender<SensorReply>,
    ) -> Result<JoinHandle<Result<()>>> {
        let args = args
            .downcast_ref::<Powersensor3Config>()
            .context("Invalid sensor args, expected args for PowerSensor3")?;

        let args = args.clone();
        let handle = spawn(async move {
            if let Err(err) = sensor_reader(
                rx,
                tx,
                "powersensor3",
                args,
                init_powersensor3,
                |args: &Powersensor3Config,
                 sensor: &Arc<Mutex<InternalPowersensor3>>,
                 _,
                 last_time|
                 -> std::pin::Pin<
                    Box<dyn Future<Output = Result<Vec<f64>, SensorError>> + Send>,
                > {
                    Box::pin(read_powersensor3(args.clone(), sensor.clone(), last_time))
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

async fn init_powersensor3(
    args: Powersensor3Config,
) -> Result<(Arc<Mutex<InternalPowersensor3>>, Vec<String>)> {
    let sensor = InternalPowersensor3::new(&args.device)?;
    let mut sensor_names = Vec::new();
    for idx in &args.indexes {
        if *idx == -1 {
            sensor_names.push("Total".to_owned());
        } else {
            sensor_names.push(sensor.get_sensor_name(*idx)?);
        }
    }
    debug!("Powersensor3 initialized");
    Ok((Arc::new(Mutex::new(sensor)), sensor_names))
}

async fn read_powersensor3(
    args: Powersensor3Config,
    sensor: Arc<Mutex<InternalPowersensor3>>,
    last_time: Instant,
) -> Result<Vec<f64>, SensorError> {
    let sensor = sensor.lock().await;
    let start_time = Instant::now();
    let start = sensor
        .read()
        .context("Read sensor")
        .map_err(SensorError::MajorFailure)?;
    async_io::Timer::after(Duration::from_micros(min(
        1000 - start_time.elapsed().as_micros() as u64
            - (last_time.elapsed().as_micros() as u64).saturating_sub(1000),
        1000,
    )))
    .await;
    let end = sensor
        .read()
        .context("Read sensor")
        .map_err(SensorError::MajorFailure)?;

    let readings = args
        .indexes
        .iter()
        .map(|sensor_idx| start.watts(&end, Some(*sensor_idx)))
        .collect::<Vec<f64>>();
    Ok(readings)
}
