use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FioResult {
    #[serde(rename = "fio version")]
    pub fio_version: String,
    pub timestamp: i64,
    pub timestamp_ms: i64,
    pub time: String,
    pub jobs: Vec<Job>,
    pub disk_util: Option<Vec<DiskUtil>>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Job {
    pub jobname: String,
    pub groupid: i64,
    pub job_start: Option<i64>,
    pub error: i64,
    pub eta: i64,
    pub elapsed: i64,
    #[serde(rename = "job options")]
    pub job_options: JobOptions,
    pub read: Read,
    pub write: Write,
    pub trim: Trim,
    pub sync: Sync,
    pub job_runtime: i64,
    pub usr_cpu: f64,
    pub sys_cpu: f64,
    pub ctx: i64,
    pub majf: i64,
    pub minf: i64,
    pub iodepth_level: IodepthLevel,
    pub iodepth_submit: IodepthSubmit,
    pub iodepth_complete: IodepthComplete,
    pub latency_ns: LatencyNs,
    pub latency_us: LatencyUs,
    pub latency_ms: LatencyMs,
    pub latency_depth: i64,
    pub latency_target: i64,
    pub latency_percentile: f64,
    pub latency_window: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobOptions {
    pub name: String,
    pub filename: String,
    pub rw: String,
    pub direct: String,
    pub bs: Option<String>,
    pub ioengine: Option<String>,
    pub time_based: String,
    pub iodepth: Option<String>,
    pub runtime: Option<String>,
    pub ramp_time: Option<String>,
    pub size: Option<String>,
    pub write_bw_log: Option<String>,
    pub write_iops_log: Option<String>,
    pub log_avg_msec: Option<String>,
    pub write_lat_log: Option<String>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Read {
    pub io_bytes: i64,
    pub io_kbytes: i64,
    pub bw_bytes: i64,
    pub bw: i64,
    pub iops: f64,
    pub runtime: i64,
    pub total_ios: i64,
    pub short_ios: i64,
    pub drop_ios: i64,
    pub slat_ns: SlatNs,
    pub clat_ns: ClatNs,
    pub lat_ns: LatNs,
    pub bw_min: i64,
    pub bw_max: i64,
    pub bw_agg: f64,
    pub bw_mean: f64,
    pub bw_dev: f64,
    pub bw_samples: i64,
    pub iops_min: i64,
    pub iops_max: i64,
    pub iops_mean: f64,
    pub iops_stddev: f64,
    pub iops_samples: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SlatNs {
    pub min: i64,
    pub max: i64,
    pub mean: f64,
    pub stddev: f64,
    #[serde(rename = "N")]
    pub n: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatNs {
    pub min: i64,
    pub max: i64,
    pub mean: f64,
    pub stddev: f64,
    #[serde(rename = "N")]
    pub n: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Write {
    pub io_bytes: i64,
    pub io_kbytes: i64,
    pub bw_bytes: i64,
    pub bw: i64,
    pub iops: f64,
    pub runtime: i64,
    pub total_ios: i64,
    pub short_ios: i64,
    pub drop_ios: i64,
    pub slat_ns: SlatNs,
    pub clat_ns: ClatNs,
    pub lat_ns: LatNs,
    pub bw_min: i64,
    pub bw_max: i64,
    pub bw_agg: f64,
    pub bw_mean: f64,
    pub bw_dev: f64,
    pub bw_samples: i64,
    pub iops_min: i64,
    pub iops_max: i64,
    pub iops_mean: f64,
    pub iops_stddev: f64,
    pub iops_samples: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClatNs {
    pub min: i64,
    pub max: i64,
    pub mean: f64,
    pub stddev: f64,
    #[serde(rename = "N")]
    pub n: i64,
    pub percentile: Option<Percentile>,
    pub bins: Option<HashMap<String, i64>>,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Percentile {
    #[serde(rename = "1.000000")]
    pub n1_000000: i64,
    #[serde(rename = "5.000000")]
    pub n5_000000: i64,
    #[serde(rename = "10.000000")]
    pub n10_000000: i64,
    #[serde(rename = "20.000000")]
    pub n20_000000: i64,
    #[serde(rename = "30.000000")]
    pub n30_000000: i64,
    #[serde(rename = "40.000000")]
    pub n40_000000: i64,
    #[serde(rename = "50.000000")]
    pub n50_000000: i64,
    #[serde(rename = "60.000000")]
    pub n60_000000: i64,
    #[serde(rename = "70.000000")]
    pub n70_000000: i64,
    #[serde(rename = "80.000000")]
    pub n80_000000: i64,
    #[serde(rename = "90.000000")]
    pub n90_000000: i64,
    #[serde(rename = "95.000000")]
    pub n95_000000: i64,
    #[serde(rename = "99.000000")]
    pub n99_000000: i64,
    #[serde(rename = "99.500000")]
    pub n99_500000: i64,
    #[serde(rename = "99.900000")]
    pub n99_900000: i64,
    #[serde(rename = "99.950000")]
    pub n99_950000: i64,
    #[serde(rename = "99.990000")]
    pub n99_990000: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Trim {
    pub io_bytes: i64,
    pub io_kbytes: i64,
    pub bw_bytes: i64,
    pub bw: i64,
    pub iops: f64,
    pub runtime: i64,
    pub total_ios: i64,
    pub short_ios: i64,
    pub drop_ios: i64,
    pub slat_ns: SlatNs,
    pub clat_ns: ClatNs,
    pub lat_ns: LatNs,
    pub bw_min: i64,
    pub bw_max: i64,
    pub bw_agg: f64,
    pub bw_mean: f64,
    pub bw_dev: f64,
    pub bw_samples: i64,
    pub iops_min: i64,
    pub iops_max: i64,
    pub iops_mean: f64,
    pub iops_stddev: f64,
    pub iops_samples: i64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sync {
    pub total_ios: i64,
    pub lat_ns: LatNs,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IodepthLevel {
    #[serde(rename = "1")]
    pub n1: f64,
    #[serde(rename = "2")]
    pub n2: f64,
    #[serde(rename = "4")]
    pub n4: f64,
    #[serde(rename = "8")]
    pub n8: f64,
    #[serde(rename = "16")]
    pub n16: f64,
    #[serde(rename = "32")]
    pub n32: f64,
    #[serde(rename = ">=64")]
    pub n64: f64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IodepthSubmit {
    #[serde(rename = "0")]
    pub n0: f64,
    #[serde(rename = "4")]
    pub n4: f64,
    #[serde(rename = "8")]
    pub n8: f64,
    #[serde(rename = "16")]
    pub n16: f64,
    #[serde(rename = "32")]
    pub n32: f64,
    #[serde(rename = "64")]
    pub n64: f64,
    #[serde(rename = ">=64")]
    pub n642: f64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IodepthComplete {
    #[serde(rename = "0")]
    pub n0: f64,
    #[serde(rename = "4")]
    pub n4: f64,
    #[serde(rename = "8")]
    pub n8: f64,
    #[serde(rename = "16")]
    pub n16: f64,
    #[serde(rename = "32")]
    pub n32: f64,
    #[serde(rename = "64")]
    pub n64: f64,
    #[serde(rename = ">=64")]
    pub n642: f64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatencyNs {
    #[serde(rename = "2")]
    pub n2: f64,
    #[serde(rename = "4")]
    pub n4: f64,
    #[serde(rename = "10")]
    pub n10: f64,
    #[serde(rename = "20")]
    pub n20: f64,
    #[serde(rename = "50")]
    pub n50: f64,
    #[serde(rename = "100")]
    pub n100: f64,
    #[serde(rename = "250")]
    pub n250: f64,
    #[serde(rename = "500")]
    pub n500: f64,
    #[serde(rename = "750")]
    pub n750: f64,
    #[serde(rename = "1000")]
    pub n1000: f64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatencyUs {
    #[serde(rename = "2")]
    pub n2: f64,
    #[serde(rename = "4")]
    pub n4: f64,
    #[serde(rename = "10")]
    pub n10: f64,
    #[serde(rename = "20")]
    pub n20: f64,
    #[serde(rename = "50")]
    pub n50: f64,
    #[serde(rename = "100")]
    pub n100: f64,
    #[serde(rename = "250")]
    pub n250: f64,
    #[serde(rename = "500")]
    pub n500: f64,
    #[serde(rename = "750")]
    pub n750: f64,
    #[serde(rename = "1000")]
    pub n1000: f64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LatencyMs {
    #[serde(rename = "2")]
    pub n2: f64,
    #[serde(rename = "4")]
    pub n4: f64,
    #[serde(rename = "10")]
    pub n10: f64,
    #[serde(rename = "20")]
    pub n20: f64,
    #[serde(rename = "50")]
    pub n50: f64,
    #[serde(rename = "100")]
    pub n100: f64,
    #[serde(rename = "250")]
    pub n250: f64,
    #[serde(rename = "500")]
    pub n500: f64,
    #[serde(rename = "750")]
    pub n750: f64,
    #[serde(rename = "1000")]
    pub n1000: f64,
    #[serde(rename = "2000")]
    pub n2000: f64,
    #[serde(rename = ">=2000")]
    pub n20002: f64,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DiskUtil {
    pub name: String,
    pub read_ios: i64,
    pub write_ios: i64,
    pub read_sectors: Option<i64>,
    pub write_sectors: Option<i64>,
    pub read_merges: i64,
    pub write_merges: i64,
    pub read_ticks: i64,
    pub write_ticks: i64,
    pub in_queue: i64,
    pub util: f64,
}
