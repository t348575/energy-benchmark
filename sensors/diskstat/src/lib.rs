use std::{
    cmp::min,
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
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, read_to_string},
    io::{AsyncReadExt, AsyncSeekExt},
    spawn,
    sync::Mutex,
    task::JoinHandle,
};
use tracing::error;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct DiskStatConfig;

#[typetag::serde]
impl SensorArgs for DiskStatConfig {
    fn name(&self) -> &'static str {
        "DiskStat"
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
struct InternalDiskStatConfig {
    device: String,
}

#[typetag::serde]
impl SensorArgs for InternalDiskStatConfig {
    fn name(&self) -> &'static str {
        "DiskStat"
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
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
    read_merges: u64,
    write_merges: u64,
    time_in_queue: u64,
    read_ticks: u64,
    write_ticks: u64,
}

const DISKSTAT_FILENAME: &str = "diskstat.csv";

impl Sensor for Diskstat {
    fn name(&self) -> &'static str {
        "DiskStat"
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
                 sensor: &Arc<Mutex<InternalDiskStat>>,
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
    let file = File::open(format!("/sys/block/{}/stat", config.device)).await?;
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
    let start_time = Instant::now();
    let readings = sensor.read(&last_time).await?;
    async_io::Timer::after(Duration::from_micros(min(
        10000
            - start_time.elapsed().as_micros() as u64
            - (last_time.elapsed().as_micros() as u64).saturating_sub(10000),
        10000,
    )))
    .await;

    Ok(readings)
}

impl InternalDiskStat {
    async fn read(&mut self, prev_time: &Instant) -> ReadDiskResult {
        let mut buf = String::new();
        self.file
            .seek(std::io::SeekFrom::Start(0))
            .await
            .context("Seek to start")
            .map_err(SensorError::MajorFailure)?;
        self.file
            .read_to_string(&mut buf)
            .await
            .context("Read to string")
            .map_err(SensorError::MajorFailure)?;

        let fields: Vec<u64> = buf.split_whitespace().map(|s| s.parse().unwrap()).collect();

        let reads = fields[2];
        let writes = fields[6];
        let readings = vec![
            reads as f64,
            writes as f64,
            ((reads as f64 - self.prev.read as f64) * self.hw_sector_size as f64)
                / prev_time.elapsed().as_secs_f64(),
            ((writes as f64 - self.prev.write as f64) * self.hw_sector_size as f64)
                / prev_time.elapsed().as_secs_f64(),
            fields[0] as f64,
            fields[4] as f64,
            fields[1] as f64,
            fields[5] as f64,
            fields[10] as f64,
            fields[3] as f64,
            fields[7] as f64,
        ];

        self.prev.read = reads;
        self.prev.write = writes;
        self.prev.read_merges = fields[2];
        self.prev.write_merges = fields[3];
        self.prev.time_in_queue = fields[4];
        self.prev.read_ticks = fields[5];
        self.prev.write_ticks = fields[6];

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
