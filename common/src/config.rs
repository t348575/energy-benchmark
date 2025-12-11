use std::{
    fmt::{Debug, Write},
    path::Path,
};

use eyre::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::{
    bench::{Bench, BenchArgs},
    plot::Plot,
    sensor::SensorArgs,
    util::write_one_line,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub name: String,
    pub settings: Settings,
    pub benches: Vec<InnerBench>,
    pub bench_args: Vec<Box<dyn BenchArgs>>,
    pub sensors: Vec<Sensor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Sensor {
    pub sensor: sensor_common::SensorKind,
    pub args: Option<Box<dyn SensorArgs>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Settings {
    pub numa: Option<NumaConfig>,
    pub device: String,
    pub nvme_power_states: Option<Vec<usize>>,
    pub max_repeat: Option<usize>,
    pub should_trace: Option<bool>,
    pub cpu_freq: Option<CpuFreq>,
    pub cpu_max_power_watts: f64,
    pub cgroup: Option<Cgroup>,
    pub sleep_between_experiments: Option<u64>,
    pub sleep_after_writes: Option<u64>,
    pub scheduler: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Cgroup {
    pub io: Option<CgroupIo>,
    pub cpuset: Option<CgroupCpuSet>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CgroupCpuSet {
    pub cpus: Option<Vec<CgroupRange>>,
    pub mems: Option<Vec<CgroupRange>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CgroupRange(u32, Option<u32>);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CgroupIo {
    pub max: Option<CgroupIoLimit>,
    pub weight: Option<usize>,
    pub latency: Option<usize>,
    pub cost: Option<CgroupIoCost>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CgroupIoLimit {
    pub bps: Option<OptionalRwIos>,
    pub iops: Option<OptionalRwIos>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CgroupIoCost {
    pub qos: Option<CgroupIoCostQos>,
    pub model: Option<CgroupIoCostModel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CgroupIoCostQos {
    Auto,
    User {
        pct: RwIos,
        latency: RwIos,
        scaling: MinMax,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CgroupIoCostModel {
    Auto,
    User {
        bps: RwIos,
        seqiops: RwIos,
        randiops: RwIos,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MinMax {
    pub min: u64,
    pub max: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RwIos {
    pub r: u64,
    pub w: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptionalRwIos {
    pub r: Option<u64>,
    pub w: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CpuFreq {
    pub freq: usize,
    pub default_governor: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NumaConfig {
    pub cpunodebind: usize,
    pub membind: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InnerBench {
    pub name: String,
    pub repeat: usize,
    pub bench: Box<dyn Bench>,
    pub plots: Option<Vec<Box<dyn Plot>>>,
}

impl RwIos {
    fn fmt(&self, names: (&str, &str)) -> String {
        format!("{}={} {}={}", names.0, self.r, names.1, self.w)
    }
}

impl MinMax {
    fn fmt(&self) -> String {
        format!("min={} max={}", self.min, self.max)
    }
}

impl OptionalRwIos {
    fn fmt(&self, names: (&str, &str)) -> Result<String> {
        let mut s = String::new();
        if let Some(r) = self.r {
            write!(&mut s, "{}={}", names.0, r)?;
        }
        if let Some(w) = self.w {
            if !s.is_empty() {
                s.push(' ');
            }
            write!(&mut s, "{}={}", names.1, w)?;
        }
        Ok(s)
    }
}

impl Cgroup {
    pub async fn apply<P: AsRef<Path>, S: AsRef<str>>(
        &self,
        cg_path: P,
        device: S,
        apply_qos_model: bool,
    ) -> Result<()> {
        if let Some(io) = &self.io {
            io.apply(&cg_path, device, apply_qos_model).await?;
        }

        if let Some(cpuset) = &self.cpuset {
            cpuset.apply(&cg_path).await?;
        }
        Ok(())
    }
}

impl CgroupCpuSet {
    pub async fn apply<P: AsRef<Path>>(&self, cg_path: P) -> Result<()> {
        let base = cg_path.as_ref();
        if let Some(mems) = &self.mems {
            let mem_str = mems
                .iter()
                .map(|x| {
                    if x.1.is_some() {
                        format!("{}-{}", x.0, x.1.as_ref().unwrap())
                    } else {
                        format!("{}", x.0)
                    }
                })
                .collect::<Vec<_>>()
                .join(",");
            write_one_line(base.join("cpuset.mems"), &mem_str).await?;
        }

        if let Some(cpus) = &self.cpus {
            let cpu_str = cpus
                .iter()
                .map(|x| {
                    if x.1.is_some() {
                        format!("{}-{}", x.0, x.1.as_ref().unwrap())
                    } else {
                        format!("{}", x.0)
                    }
                })
                .collect::<Vec<_>>()
                .join(",");
            write_one_line(base.join("cpuset.cpus"), &cpu_str).await?;
        }
        Ok(())
    }
}

impl CgroupIo {
    pub async fn apply<P: AsRef<Path>, S: AsRef<str>>(
        &self,
        cg_path: P,
        device: S,
        apply_qos_model: bool,
    ) -> Result<()> {
        let base = cg_path.as_ref();

        let cmd = format!("{} ", device.as_ref().trim());
        if let Some(max) = &self.max {
            let mut cmd = cmd.clone();
            if let Some(bps) = &max.bps {
                cmd.write_str(&bps.fmt(("rbps", "wbps"))?)?;
            }

            if let Some(iops) = &max.iops {
                write!(&mut cmd, " {}", iops.fmt(("riops", "wiops"))?)?;
            }

            if !cmd.is_empty() {
                write_one_line(base.join("io.max"), &cmd).await?;
            }
        }

        if let Some(weight) = self.weight {
            let mut cmd = cmd.clone();
            write!(&mut cmd, " {weight}")?;
            write_one_line(base.join("io.weight"), &cmd).await?;
        }

        if let Some(latency) = self.latency {
            let mut cmd = cmd.clone();
            write!(&mut cmd, " {latency}")?;
            write_one_line(base.join("io.latency"), &cmd).await?;
        }

        if !apply_qos_model {
            return Ok(());
        }

        if let Some(cost) = &self.cost {
            if let Some(qos) = &cost.qos {
                let mut cmd = format!("{cmd} enable=1 ctrl=user ");
                match qos {
                    CgroupIoCostQos::Auto => {}
                    CgroupIoCostQos::User {
                        pct,
                        latency: lat,
                        scaling,
                    } => {
                        cmd.write_str(&pct.fmt(("rpct", "wpct")))?;
                        write!(&mut cmd, " {}", &lat.fmt(("rlat", "wlat")))?;
                        write!(&mut cmd, " {}", &scaling.fmt())?;
                        write_one_line(base.parent().unwrap().join("io.cost.qos"), &cmd).await?;
                    }
                }
            }

            if let Some(model) = &cost.model {
                let mut cmd = cmd.clone();
                match model {
                    CgroupIoCostModel::Auto => {}
                    CgroupIoCostModel::User {
                        bps,
                        seqiops,
                        randiops,
                    } => {
                        cmd.write_str(&bps.fmt(("rbps", "wbps")))?;
                        write!(&mut cmd, " {}", &seqiops.fmt(("rseqiops", "wseqiops")))?;
                        write!(&mut cmd, " {}", &randiops.fmt(("rrandiops", "wrandiops")))?;
                        write_one_line(base.parent().unwrap().join("io.cost.model"), &cmd)
                            .await
                            .context("Write io.cost.model file")?;
                    }
                }
            }
        }

        Ok(())
    }
}
