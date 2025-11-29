use std::path::PathBuf;

use eyre::Result;
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MlperfMetrics {
    pub metric: Metric,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metric {
    pub train_au_percentage: Vec<f64>,
    pub train_au_mean_percentage: f64,
    pub train_au_meet_expectation: String,
    pub train_au_stdev_percentage: f64,
    pub train_throughput_samples_per_second: Vec<f64>,
    pub train_throughput_mean_samples_per_second: f64,
    pub train_throughput_stdev_samples_per_second: f64,
    #[serde(rename = "train_io_mean_MB_per_second")]
    pub train_io_mean_mb_per_second: f64,
    #[serde(rename = "train_io_stdev_MB_per_second")]
    pub train_io_stdev_mb_per_second: f64,
}

pub fn find_summary(base: &PathBuf) -> Option<PathBuf> {
    WalkDir::new(base)
        .min_depth(4)
        .max_depth(4)
        .into_iter()
        .filter_map(Result::ok)
        .find(|e| e.file_type().is_file() && e.file_name() == "summary.json")
        .map(|e| e.into_path())
}
