use std::{
    fs::File,
    os::unix::fs::FileExt,
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use common::{
    config::Settings,
    sensor::{Sensor, SensorArgs, SensorReply, SensorRequest},
    util::{SensorError, TimeSeriesAxis, sensor_reader},
};
use eyre::{Context, Result};
use flume::{Receiver, Sender};
use sensor_common::SensorKind;
use serde::{Deserialize, Serialize};
use tokio::{fs::read_to_string, spawn, sync::Mutex, task::JoinHandle};
use tracing::error;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiskstatConfig;

#[typetag::serde]
impl SensorArgs for DiskstatConfig {
    fn name(&self) -> SensorKind {
        SensorKind::Diskstat
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct InternalDiskStatConfig {
    device: String,
}

#[typetag::serde]
impl SensorArgs for InternalDiskStatConfig {
    fn name(&self) -> SensorKind {
        SensorKind::Diskstat
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Diskstat;

struct InternalDiskStat {
    file: File,
    hw_sector_size: u64,
    prev: DiskStatData,
}

#[derive(Default)]
struct DiskStatData {
    read: u64,
    write: u64,
}

const DISKSTAT_FILENAME: &str = "diskstat.csv";

impl Sensor for Diskstat {
    fn name(&self) -> SensorKind {
        SensorKind::Diskstat
    }

    fn filename(&self) -> &'static str {
        DISKSTAT_FILENAME
    }

    fn start(
        &self,
        _: &dyn SensorArgs,
        settings: &Settings,
        rx: Receiver<SensorRequest>,
        tx: Sender<SensorReply>,
    ) -> Result<JoinHandle<Result<()>>> {
        let device = settings.device.strip_prefix("/dev/").unwrap().to_string();
        let handle = spawn(async move {
            if let Err(err) = sensor_reader(
                rx,
                tx,
                DISKSTAT_FILENAME,
                InternalDiskStatConfig { device },
                init_diskstat,
                |_,
                 sensor,
                 _,
                 last_time|
                 -> std::pin::Pin<
                    Box<dyn Future<Output = Result<Vec<f64>, SensorError>> + Send>,
                > { Box::pin(read_diskstat(sensor.clone(), last_time)) },
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

async fn init_diskstat(
    config: InternalDiskStatConfig,
) -> Result<(Arc<Mutex<InternalDiskStat>>, Vec<String>)> {
    let hw_sector_size =
        read_to_string(format!("/sys/block/{}/queue/hw_sector_size", config.device))
            .await?
            .trim()
            .parse()?;
    let file = File::open(format!("/sys/block/{}/stat", config.device))?;
    let mut diskstat = InternalDiskStat {
        file,
        hw_sector_size,
        prev: DiskStatData::default(),
    };
    _ = diskstat.read(&Instant::now()).await?;
    Ok((
        Arc::new(Mutex::new(diskstat)),
        vec![
            "bytes_read",
            "bytes_write",
            "read",
            "write",
            "read_ios",
            "write_ios",
            "read_merges",
            "write_merges",
            "time_in_queue",
            "read_ticks",
            "write_ticks",
        ]
        .into_iter()
        .map(|x| x.to_owned())
        .collect(),
    ))
}

type ReadDiskResult = Result<Vec<f64>, SensorError>;
async fn read_diskstat(sensor: Arc<Mutex<InternalDiskStat>>, last_time: Instant) -> ReadDiskResult {
    let mut sensor = sensor.lock().await;
    let readings = sensor.read(&last_time).await?;
    async_io::Timer::after(Duration::from_micros(10000)).await;
    Ok(readings)
}

impl InternalDiskStat {
    async fn read(&mut self, prev_time: &Instant) -> ReadDiskResult {
        let mut buf = [0u8; 256];
        let read = self
            .file
            .read_at(&mut buf, 0)
            .context("Read stat file")
            .map_err(|e| SensorError::MajorFailure(e))?;

        let mut fields: [&[u8]; 17] = [&[]; 17];
        let mut n = 0usize;
        let mut i = 0usize;
        let mut in_token = false;

        while i < read {
            let b = buf[i];
            let is_ws = matches!(b, b' ' | b'\t' | b'\n' | b'\r');
            if !is_ws && !in_token {
                let start = i;
                let mut j = i + 1;
                while j < buf.len() {
                    let c = buf[j];
                    if matches!(c, b' ' | b'\t' | b'\n' | b'\r') {
                        break;
                    }
                    j += 1;
                }
                if n < fields.len() {
                    fields[n] = &buf[start..j];
                    n += 1;
                }
                i = j;
                in_token = false;
            } else {
                i += 1;
            }
        }

        use atoi::FromRadix10;
        let reads = u64::from_radix_10(fields[2]).0;
        let writes = u64::from_radix_10(fields[6]).0;
        let readings = vec![
            reads as f64,
            writes as f64,
            ((reads as f64 - self.prev.read as f64) * self.hw_sector_size as f64)
                / prev_time.elapsed().as_secs_f64(),
            ((writes as f64 - self.prev.write as f64) * self.hw_sector_size as f64)
                / prev_time.elapsed().as_secs_f64(),
            u64::from_radix_10(fields[0]).0 as f64,
            u64::from_radix_10(fields[4]).0 as f64,
            u64::from_radix_10(fields[1]).0 as f64,
            u64::from_radix_10(fields[5]).0 as f64,
            u64::from_radix_10(fields[10]).0 as f64,
            u64::from_radix_10(fields[3]).0 as f64,
            u64::from_radix_10(fields[7]).0 as f64,
        ];

        self.prev.read = reads;
        self.prev.write = writes;

        Ok(readings)
    }
}

pub static DISKSTAT_PLOT_AXIS: LazyLock<[TimeSeriesAxis; 3]> = LazyLock::new(|| {
    [
        TimeSeriesAxis::sensor(DISKSTAT_FILENAME, "total", "diskstat", "Throughput (MiB/s)"),
        TimeSeriesAxis::sensor(
            DISKSTAT_FILENAME,
            "read",
            "diskstat read",
            "Throughput (MiB/s)",
        ),
        TimeSeriesAxis::sensor(
            DISKSTAT_FILENAME,
            "write",
            "diskstat write",
            "Throughput (MiB/s)",
        ),
    ]
});
