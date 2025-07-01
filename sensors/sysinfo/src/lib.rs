use std::{
    cmp::min,
    sync::Arc,
    time::{Duration, Instant},
};

use common::{
    sensor::{Sensor, SensorArgs, SensorReply, SensorRequest},
    util::sensor_reader,
};
use eyre::{ContextCompat, Result};
use flume::{Receiver, Sender};
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
    fn name(&self) -> &'static str {
        "Sysinfo"
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Sysinfo;

impl Sensor for Sysinfo {
    fn name(&self) -> &'static str {
        "Sysinfo"
    }

    fn start(
        &self,
        args: &dyn SensorArgs,
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
                "sysinfo",
                args,
                init_sysinfo,
                |args: &SysinfoConfig, sensor: &Arc<Mutex<System>>, request: &SensorRequest, _| -> std::pin::Pin<Box<dyn Future<Output = Result<Vec<f64>>> + Send>> {
                    match request {
                        SensorRequest::StartRecording { pid, .. } => Box::pin(read_sysinfo(args.clone(), sensor.clone(), *pid)),
                        _ => unreachable!()
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
) -> Result<Vec<f64>> {
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
    .await?;

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
