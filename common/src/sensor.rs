use core::fmt::Debug;
use std::path::PathBuf;

use downcast_rs::{Downcast, impl_downcast};
use dyn_clone::{DynClone, clone_trait_object};
use eyre::Result;
use flume::{Receiver, Sender};
use tokio::task::JoinHandle;

use crate::config::Settings;

#[derive(Debug)]
pub enum SensorRequest {
    StartRecording {
        dir: PathBuf,
        args: Vec<String>,
        program: String,
        pid: u32,
        bench: Box<dyn crate::bench::Bench>,
    },
    StopRecording,
    /// Quit the spawned [`tokio::task`]
    Quit,
}

pub enum SensorReply {
    FileDumpComplete,
}

/// All [`Sensor`] implementations are expected to implement [`Default`]
pub trait Sensor: Debug + Send + Sync {
    /// Name of the sensor, for identification
    fn name(&self) -> sensor_common::SensorKind;
    /// Sensor data filename
    fn filename(&self) -> &'static str;
    /// Should start an async task that collects sensor data using [`tokio::task::spawn`]
    ///
    /// Arguments:
    /// * `args` - Specific arguments to the sensor
    /// * `rx` - Requests to the sensor to start/stop recording
    /// * `tx` - Replies from the sensor when its done flushing data to disk, after [`SensorRequest::StopRecording`] is received
    fn start(
        &self,
        args: &dyn SensorArgs,
        settings: &Settings,
        rx: Receiver<SensorRequest>,
        tx: Sender<SensorReply>,
    ) -> Result<JoinHandle<Result<()>>>;
}

#[typetag::serde(tag = "type")]
/// All [`SensorArgs`] implementations are expected to implement [`Default`]
pub trait SensorArgs: Debug + DynClone + Downcast + Send + Sync {
    fn name(&self) -> sensor_common::SensorKind;
}
clone_trait_object!(SensorArgs);
impl_downcast!(SensorArgs);
