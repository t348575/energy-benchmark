use std::{
    cmp::min,
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use common::{
    config::Settings,
    sensor::{Sensor, SensorArgs, SensorReply, SensorRequest},
    util::{SensorError, TimeSeriesAxis, sensor_reader},
};
use eyre::{Context, ContextCompat, Result};
use flume::{Receiver, Sender};
use sensor_common::SensorKind;
use serde::{Deserialize, Serialize};
use sysinfo::{MemoryRefreshKind, Pid, ProcessRefreshKind, System};
use tokio::{
    spawn,
    sync::Mutex,
    task::{JoinHandle, spawn_blocking},
    time::sleep,
};
use tracing::error;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct SysinfoConfig {
    pub interval: u64,
}

#[typetag::serde]
impl SensorArgs for SysinfoConfig {
    fn name(&self) -> SensorKind {
        SensorKind::Sysinfo
    }
}

const SYSINFO_FILENAME: &str = "sysinfo.csv";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Sysinfo;

impl Sensor for Sysinfo {
    fn name(&self) -> SensorKind {
        SensorKind::Sysinfo
    }

    fn filename(&self) -> &'static str {
        SYSINFO_FILENAME
    }

    fn start(
        &self,
        args: &dyn SensorArgs,
        _: &Settings,
        rx: Receiver<SensorRequest>,
        tx: Sender<SensorReply>,
    ) -> Result<JoinHandle<Result<()>>> {
        let args = args
            .downcast_ref::<SysinfoConfig>()
            .context("Invalid sensor args, expected args for Sysinfo")?;

        let args = args.clone();
        let handle = spawn(async move {
            if let Err(err) = sensor_reader(
                rx,
                tx,
                SYSINFO_FILENAME,
                args,
                init_sysinfo,
                |args: &SysinfoConfig,
                 sensor: &Arc<Mutex<System>>,
                 request: &SensorRequest,
                 _|
                 -> std::pin::Pin<
                    Box<dyn Future<Output = Result<Vec<f64>, SensorError>> + Send>,
                > {
                    match request {
                        SensorRequest::StartRecording { pid, .. } => {
                            Box::pin(read_sysinfo(args.clone(), sensor.clone(), *pid))
                        }
                        _ => unreachable!(),
                    }
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

async fn init_sysinfo(_: SysinfoConfig) -> Result<(Arc<Mutex<System>>, Vec<String>)> {
    let mut sys = System::new_all();
    sys.refresh_all();

    let num_cpus = sys.cpus().len();
    let cpu_names = (0..num_cpus)
        .map(|x| format!("cpu-{x}-freq"))
        .collect::<Vec<_>>();
    let load_names = (0..num_cpus)
        .map(|x| format!("cpu-{x}-load"))
        .collect::<Vec<_>>();

    Ok((
        Arc::new(Mutex::new(sys)),
        cpu_names
            .into_iter()
            .chain(load_names)
            .chain([
                "mem".to_owned(),
                "bench-cpu".to_owned(),
                "bench-mem".to_owned(),
            ])
            .collect(),
    ))
}

async fn read_sysinfo(
    config: SysinfoConfig,
    sensor: Arc<Mutex<System>>,
    pid: u32,
) -> Result<Vec<f64>, SensorError> {
    let start = Instant::now();
    let (cpu_freq, load, mem, fio_cpu, fio_mem) = spawn_blocking(move || {
        let mut sys = sensor.blocking_lock();
        sys.refresh_cpu_all();
        let cpu_freq = sys
            .cpus()
            .iter()
            .map(|cpu| cpu.frequency())
            .collect::<Vec<_>>();
        let load = sys
            .cpus()
            .iter()
            .map(|cpu| cpu.cpu_usage())
            .collect::<Vec<_>>();

        sys.refresh_memory_specifics(MemoryRefreshKind::nothing().with_ram());
        let mem = sys.used_memory();

        sys.refresh_processes_specifics(
            sysinfo::ProcessesToUpdate::Some(&[Pid::from_u32(pid)]),
            false,
            ProcessRefreshKind::nothing().with_cpu().with_memory(),
        );
        let target_process = sys.process(Pid::from_u32(pid)).unwrap();
        let target_process_cpu = target_process.cpu_usage();
        let target_process_mem = target_process.memory();
        drop(sys);
        (cpu_freq, load, mem, target_process_cpu, target_process_mem)
    })
    .await
    .context("Fetching sysinfo")
    .map_err(SensorError::MajorFailure)?;

    sleep(Duration::from_micros(min(
        (config.interval * 1000) - start.elapsed().as_micros() as u64,
        config.interval * 1000,
    )))
    .await;
    let mut readings = cpu_freq
        .into_iter()
        .map(|x| x as f64)
        .chain(load.into_iter().map(|x| x as f64))
        .collect::<Vec<_>>();
    readings.extend_from_slice(&[mem as f64, fio_cpu as f64, fio_mem as f64]);
    Ok(readings)
}

pub fn sysinfo_freq_plot_axis(cpu_topology: &HashMap<u32, u32>) -> Vec<TimeSeriesAxis> {
    cpu_topology
        .iter()
        .map(|x| {
            TimeSeriesAxis::sensor(
                SYSINFO_FILENAME,
                format!("average_freq_node{}", x.0),
                format!("CPU {} Freq.", x.0),
                "CPU Freq. (MHz)",
            )
        })
        .collect()
}

pub fn sysinfo_load_plot_axis(cpu_topology: &HashMap<u32, u32>) -> Vec<TimeSeriesAxis> {
    cpu_topology
        .iter()
        .map(|x| {
            TimeSeriesAxis::sensor(
                SYSINFO_FILENAME,
                format!("average_load_node{}", x.0),
                format!("CPU {} Load.", x.0),
                "CPU Load.",
            )
        })
        .collect()
}
