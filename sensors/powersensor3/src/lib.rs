#[cxx::bridge(namespace = "powersensor3_rs")]
mod ffi {
    unsafe extern "C++" {
        include!("PowerSensor.hpp");
        include!("powersensor3/src/wrapper/wrapper.hpp");

        type PowerSensor;
        type State;

        fn create(device: &str) -> Result<UniquePtr<PowerSensor>>;
        fn read(sensor: &PowerSensor) -> Result<UniquePtr<State>>;
        fn calculate_watts(start: &State, end: &State, pair_id: i32) -> f64;
        fn get_sensor_name(sensor: &PowerSensor, sensor_id: i32) -> Result<String>;
    }
}

use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use common::{
    config::Settings,
    sensor::{Sensor, SensorArgs, SensorReply, SensorRequest},
    util::{SensorError, TimeSeriesAxis, sensor_reader},
};
use cxx::UniquePtr;
use eyre::{Context, ContextCompat, Result};
use flume::{Receiver, Sender};
use sensor_common::SensorKind;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{spawn, task::JoinHandle};
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
}

#[typetag::serde]
impl SensorArgs for Powersensor3Config {
    fn name(&self) -> SensorKind {
        SensorKind::Powersensor3
    }
}

const POWERSENSOR_FILENAME: &str = "powersensor3.csv";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Powersensor3;

impl Sensor for Powersensor3 {
    fn name(&self) -> SensorKind {
        SensorKind::Powersensor3
    }

    fn filename(&self) -> &'static str {
        POWERSENSOR_FILENAME
    }

    fn start(
        &self,
        args: &dyn SensorArgs,
        _: &Settings,
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
                POWERSENSOR_FILENAME,
                args,
                init_powersensor3,
                |_: &Powersensor3Config,
                 sensor: &mut InternalPowersensor3,
                 _,
                 last_time|
                 -> std::pin::Pin<
                    Box<dyn Future<Output = Result<Vec<f64>, SensorError>> + Send>,
                > { Box::pin(read_powersensor3(sensor, last_time)) },
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
) -> Result<(InternalPowersensor3, Vec<String>)> {
    let sensor = InternalPowersensor3::new(&args.device)?;
    let mut sensor_names = vec!["Total".to_owned()];
    sensor_names.push(sensor.get_sensor_name(1)?);
    sensor_names.push(sensor.get_sensor_name(2)?);
    debug!("Powersensor3 initialized");
    Ok((sensor, sensor_names))
}

async fn read_powersensor3(
    sensor: &InternalPowersensor3,
    _: Instant,
) -> Result<Vec<f64>, SensorError> {
    let start = sensor
        .read()
        .context("Read sensor")
        .map_err(SensorError::MajorFailure)?;
    async_io::Timer::after(Duration::from_micros(1000)).await;
    let end = sensor
        .read()
        .context("Read sensor")
        .map_err(SensorError::MajorFailure)?;

    Ok(vec![
        start.watts(&end, Some(-1)),
        start.watts(&end, Some(1)),
        start.watts(&end, Some(2)),
    ])
}

pub static POWERSENSOR_PLOT_AXIS: LazyLock<[TimeSeriesAxis; 1]> = LazyLock::new(|| {
    [TimeSeriesAxis::sensor(
        POWERSENSOR_FILENAME,
        "total_smoothed",
        "SSD Power",
        "SSD Power (Watts)",
    )]
});
