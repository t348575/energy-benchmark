use eyre::{ContextCompat, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpStat {
    pub name: String,
    pub ops: u64,
    pub ops_per_sec: f64,
    pub mb_per_sec: f64,
    pub ms_per_op: f64,
    pub us_per_op_cpu: f64,
    pub latency_min_ms: f64,
    pub latency_max_ms: f64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IoSummary {
    pub total_ops: u64,
    pub ops_per_sec: f64,
    pub read_ops_per_sec: u64,
    pub write_ops_per_sec: u64,
    pub mb_per_sec: f64,
    pub cpu_per_op_us: f64,
    pub latency_ms: f64,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FilebenchSummary {
    pub summary: IoSummary,
    pub ops_stats: Vec<OpStat>,
}

impl OpStat {
    pub fn from_line(line: &str) -> Option<Self> {
        let tokens: Vec<&str> = line.split_whitespace().collect();
        if tokens.len() < 9 {
            return None;
        }

        let name = tokens[0].to_string();

        let ops = tokens[1].trim_end_matches("ops").parse::<u64>().ok()?;
        let ops_per_sec = tokens[2].trim_end_matches("ops/s").parse::<f64>().ok()?;
        let mb_per_sec = tokens[3].trim_end_matches("mb/s").parse::<f64>().ok()?;
        let ms_per_op = tokens[4].trim_end_matches("ms/op").parse::<f64>().ok()?;
        let us_per_op_cpu = tokens[5]
            .trim_end_matches("us/op-cpu")
            .parse::<f64>()
            .ok()?;

        let min_latency = tokens[6]
            .trim_start_matches('[')
            .trim_end_matches("ms")
            .parse::<f64>()
            .ok()?;
        let max_latency = tokens[8]
            .trim_end_matches(']')
            .trim_end_matches("ms")
            .parse::<f64>()
            .ok()?;

        Some(Self {
            name,
            ops,
            ops_per_sec,
            mb_per_sec,
            ms_per_op,
            us_per_op_cpu,
            latency_min_ms: min_latency,
            latency_max_ms: max_latency,
        })
    }
}

impl IoSummary {
    pub fn from_line(line: &str) -> Option<Self> {
        let line = line.strip_prefix("IO Summary: ")?;
        let parts: Vec<&str> = line.split(',').map(|s| s.trim()).collect();
        if parts.len() != 6 {
            return None;
        }

        let total_ops = parts[0].split_whitespace().next()?.parse::<u64>().ok()?;
        let ops_per_sec = parts[1].split_whitespace().next()?.parse::<f64>().ok()?;

        let rw_part = parts[2].trim_matches(|c| c == '(' || c == ')');
        let rw_parts: Vec<&str> = rw_part.split_whitespace().collect();
        let ratios = rw_parts[0].split('/').collect::<Vec<&str>>();
        let read_ops_per_sec = ratios[0].parse::<u64>().ok()?;
        let write_ops_per_sec = ratios[1].parse::<u64>().ok()?;

        let mb_per_sec = parts[3].split_once("mb/s")?.0.parse::<f64>().ok()?;

        let cpu_per_op_us = parts[4]
            .split_whitespace()
            .next()?
            .trim_end_matches("us")
            .parse::<f64>()
            .ok()?;

        let latency_ms = parts[5]
            .split_whitespace()
            .next()?
            .trim_end_matches("ms")
            .parse::<f64>()
            .ok()?;

        Some(Self {
            total_ops,
            ops_per_sec,
            read_ops_per_sec,
            write_ops_per_sec,
            mb_per_sec,
            cpu_per_op_us,
            latency_ms,
        })
    }
}

pub fn parse_output(output: &str) -> Result<(IoSummary, Vec<OpStat>)> {
    let mut parsing_ops = false;
    let mut ops_stats = Vec::new();
    let mut io_summary = None;
    for line in output.lines() {
        let parts: Vec<&str> = line.splitn(3, ':').collect();
        if parts.len() < 3 && !parsing_ops {
            continue;
        }
        let mut content = if parsing_ops {
            parts[0].trim()
        } else {
            parts[2].trim()
        };

        if content.starts_with("Per-Operation Breakdown") {
            parsing_ops = true;
            continue;
        }

        if parsing_ops {
            if let Some(op_stat) = OpStat::from_line(content) {
                ops_stats.push(op_stat);
            } else {
                parsing_ops = false;
                content = parts[2].trim();
            }
        }

        if content.starts_with("IO Summary: ") {
            io_summary = IoSummary::from_line(content);
        }
    }
    Ok((io_summary.context("Failed to parse IO Summary")?, ops_stats))
}
