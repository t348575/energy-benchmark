use eyre::{ContextCompat, Result};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpMetrics {
    pub ops: u64,
    pub average_latency_us: f64,
    pub min_latency_us: u64,
    pub max_latency_us: u64,
    pub p95_latency_us: f64,
    pub p99_latency_us: f64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct YcsbMetrics {
    pub total_operations: Option<u64>,
    pub throughput_ops_sec: Option<f64>,
    pub read: Option<OpMetrics>,
    pub insert: Option<OpMetrics>,
    pub update: Option<OpMetrics>,
}

#[derive(Default)]
struct PartialOp {
    ops: Option<u64>,
    avg: Option<f64>,
    min: Option<u64>,
    max: Option<u64>,
    p95: Option<f64>,
    p99: Option<f64>,
}

impl PartialOp {
    fn build(self) -> Option<OpMetrics> {
        Some(OpMetrics {
            ops: self.ops?,
            average_latency_us: self.avg?,
            min_latency_us: self.min?,
            max_latency_us: self.max?,
            p95_latency_us: self.p95?,
            p99_latency_us: self.p99?,
        })
    }
}

macro_rules! parse_field {
    ($map:expr, $re:expr, $output:expr, $field:ident) => {
        for cap in $re.captures_iter($output) {
            let op = cap
                .get(1)
                .context(format!(
                    "Could not parse '{}' category from YCSB output",
                    stringify!($field)
                ))?
                .as_str();
            let entry = $map.entry(op).or_default();
            entry.$field = cap.get(2).and_then(|m| m.as_str().parse().ok());
        }
    };
}

pub fn parse_output(output: &str) -> Result<YcsbMetrics> {
    let re_total_ops = Regex::new(r"\[OVERALL\],\s*Operations\s*,\s*(\d+)")?;
    let re_throughput =
        Regex::new(r"\[OVERALL\],\s*Throughput\(ops/sec\)\s*,\s*([0-9]+\.?[0-9]*)")?;
    let re_ops = Regex::new(r"\[([A-Z]+)\],\s*Operations\s*,\s*(\d+)")?;
    let re_avg = Regex::new(r"\[([A-Z]+)\],\s*AverageLatency\(us\)\s*,\s*([0-9]+\.?[0-9]*)")?;
    let re_min = Regex::new(r"\[([A-Z]+)\],\s*MinLatency\(us\)\s*,\s*(\d+)")?;
    let re_max = Regex::new(r"\[([A-Z]+)\],\s*MaxLatency\(us\)\s*,\s*(\d+)")?;
    let re_p95 =
        Regex::new(r"\[([A-Z]+)\],\s*95thPercentileLatency\(us\)\s*,\s*([0-9]+\.?[0-9]*)")?;
    let re_p99 =
        Regex::new(r"\[([A-Z]+)\],\s*99thPercentileLatency\(us\)\s*,\s*([0-9]+\.?[0-9]*)")?;

    let mut total_ops = None;
    let mut throughput = None;
    let mut map: HashMap<&str, PartialOp> = HashMap::new();

    if let Some(cap) = re_total_ops.captures(output) {
        total_ops = cap.get(1).and_then(|m| m.as_str().parse().ok());
    }
    if let Some(cap) = re_throughput.captures(output) {
        throughput = cap.get(1).and_then(|m| m.as_str().parse().ok());
    }

    parse_field!(map, re_ops, output, ops);
    parse_field!(map, re_avg, output, avg);
    parse_field!(map, re_min, output, min);
    parse_field!(map, re_max, output, max);
    parse_field!(map, re_p95, output, p95);
    parse_field!(map, re_p99, output, p99);

    Ok(YcsbMetrics {
        total_operations: total_ops,
        throughput_ops_sec: throughput,
        read: map.remove("READ").and_then(|p| p.build()),
        insert: map.remove("INSERT").and_then(|p| p.build()),
        update: map.remove("UPDATE").and_then(|p| p.build()),
    })
}
