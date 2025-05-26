use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    bench::{Bench, BenchArgs},
    plot::Plot,
    sensor::SensorArgs,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub name: String,
    pub settings: Settings,
    pub benches: Vec<InnerBench>,
    pub bench_args: Vec<Box<dyn BenchArgs>>,
    pub sensors: Vec<String>,
    pub sensor_args: Vec<Box<dyn SensorArgs>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Settings {
    pub numa: Option<NumaConfig>,
    pub device: String,
    pub custom_power_state_setter: Option<bool>,
    pub nvme_power_states: Option<Vec<usize>>,
    pub nvme_cli_device: Option<String>,
    pub max_repeat: Option<usize>,
    pub env: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NumaConfig {
    pub cpunodebind: usize,
    pub membind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InnerBench {
    pub name: String,
    pub repeat: usize,
    pub bench: Box<dyn Bench>,
    pub plots: Option<Vec<Box<dyn Plot>>>,
}
