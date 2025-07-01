use std::{
    collections::HashSet,
    fmt::Debug,
    hash::Hash,
    path::{Path, PathBuf},
    pin::Pin,
    process::Stdio,
    string::FromUtf8Error,
    time::Instant,
};

use csv::{ReaderBuilder, StringRecord, Writer};
use eyre::{Context, ContextCompat, Result, bail};
use flume::{Receiver, Sender};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, create_dir_all},
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    spawn,
};
use tracing::{debug, error, info, warn};

use crate::sensor::{SensorArgs, SensorReply, SensorRequest};

pub fn find_outliers_by_stddev(data: &[f64], allowed_deviation: f64) -> Vec<usize> {
    if data.is_empty() {
        return Vec::new();
    }
    let mean: f64 = data.iter().sum::<f64>() / data.len() as f64;
    data.iter()
        .enumerate()
        .filter_map(|(i, &x)| {
            if (x - mean).abs() > allowed_deviation {
                Some(i)
            } else {
                None
            }
        })
        .collect()
}

pub fn combine_and_remove_duplicates<T: Eq + Hash>(vec1: Vec<T>, vec2: Vec<T>) -> Vec<T> {
    let set: HashSet<T> = vec1.into_iter().chain(vec2).collect();
    set.into_iter().collect()
}

pub fn remove_indices<T>(vec: &mut Vec<T>, indices: &[usize]) {
    let mut indices_sorted = indices.to_vec();
    indices_sorted.sort_unstable_by(|a, b| b.cmp(a));
    for &index in &indices_sorted {
        if index < vec.len() {
            vec.remove(index);
        }
    }
}

pub fn parse_data_size(request_size: &str) -> Result<u64> {
    let request_size = request_size.to_lowercase();
    if request_size.contains("k") {
        Ok(request_size
            .replace("k", "")
            .parse::<u64>()
            .context(format!("Parse request size: {request_size}"))?
            * 1024)
    } else if request_size.contains("m") {
        Ok(request_size
            .replace("m", "")
            .parse::<u64>()
            .context(format!("Parse request size: {request_size}"))?
            * 1024
            * 1024)
    } else {
        bail!("Unsupported request size {request_size}")
    }
}

/// Utility function to perform sensor recordings in a conventional manner
pub async fn sensor_reader<
    Args,
    Sensor,
    InitSensor,
    InitSensorFut,
    ReadSensorData,
    ReadSensorFut,
    SensorData,
>(
    rx: Receiver<SensorRequest>,
    tx: Sender<SensorReply>,
    filename: &str,
    args: Args,
    init: InitSensor,
    read: ReadSensorData,
) -> Result<()>
where
    Args: SensorArgs + Clone,
    SensorData: IntoIterator,
    SensorData::Item: ToString,
    Sensor: Clone + Send + 'static,
    InitSensor: Fn(Args) -> InitSensorFut,
    InitSensorFut: Future<Output = Result<(Sensor, Vec<String>)>> + Send + 'static,
    ReadSensorData: Fn(&Args, &Sensor, &SensorRequest, Instant) -> ReadSensorFut,
    ReadSensorFut: Future<Output = Result<SensorData>>,
{
    debug!("Spawning {} reader", args.name());
    let args_copy = args.clone();
    let (s, sensor_names) = spawn(init(args_copy)).await??;

    let mut readings = Vec::new();
    let mut is_running = false;
    let mut dir = PathBuf::new();
    let mut req = SensorRequest::StopRecording;
    let mut start_time = Instant::now();
    let mut read_time = Instant::now();
    let mut last_time = Instant::now();
    loop {
        if !is_running {
            if let Ok(request) = rx.recv_async().await {
                match request {
                    SensorRequest::StartRecording {
                        dir: _dir,
                        args: bench_args,
                        program,
                        pid,
                        bench,
                    } => {
                        debug!("Starting {} reader", args.name());
                        is_running = true;
                        dir = _dir.clone();

                        req = SensorRequest::StartRecording {
                            dir: _dir,
                            args: bench_args,
                            program,
                            pid,
                            bench,
                        };
                        start_time = Instant::now();
                        read_time = Instant::now();
                    }
                    SensorRequest::Quit => break,
                    SensorRequest::StopRecording => {
                        is_running = false;
                        warn!("Expected {} start request, got stop instead", args.name());
                    }
                }
            }
        } else {
            match read(&args, &s, &req, last_time).await {
                Ok(t) => readings.push((start_time.elapsed().as_millis(), t)),
                Err(err) => info!(
                    "Error collecting sensor data for {} {:#?}",
                    args.name(),
                    err
                ),
            }
            last_time = read_time;
            read_time = Instant::now();

            if !rx.is_empty()
                && let Ok(request) = rx.recv_async().await
            {
                match request {
                    SensorRequest::StopRecording => {
                        debug!("Stopping {} reader", args.name());
                        is_running = false;
                        let filename = dir.join(format!("{filename}.csv"));
                        let mut file = File::create(filename).await?;

                        file.write_all(format!("time,{}\n", sensor_names.join(",")).as_bytes())
                            .await?;
                        for row in readings.drain(..) {
                            file.write_all(
                                format!(
                                    "{},{}\n",
                                    row.0,
                                    row.1
                                        .into_iter()
                                        .map(|x| x.to_string())
                                        .collect::<Vec<_>>()
                                        .join(",")
                                )
                                .as_bytes(),
                            )
                            .await?;
                        }
                        file.flush().await?;
                        tx.send_async(SensorReply::FileDumpComplete).await?;
                    }
                    request => {
                        warn!(
                            "Got unexpected sensor request {request:#?} for {}",
                            args.name()
                        );
                    }
                }
            }
        }
    }
    debug!("Exiting reader for {}", args.name());
    Ok(())
}

pub fn plot_python(plot_file: &str, args: &[(&str, &str)]) -> Result<()> {
    debug!(
        "{plot_file} {}",
        args.iter()
            .map(|x| [x.0, x.1])
            .flatten()
            .collect::<Vec<_>>()
            .join(" ")
    );
    let mut child = std::process::Command::new("python3")
        .arg(format!("plots/{plot_file}.py"))
        .args(args.iter().flat_map(|(k, v)| [k, v]).collect::<Vec<_>>())
        .spawn()?;
    child.wait()?;
    Ok(())
}

pub fn get_mean_power(data: &str, column: &str) -> Result<f64> {
    let mut lines = data.lines();
    let header = lines.next().context("Missing header")?;
    let headers: Vec<&str> = header.split(',').collect();
    let selected_column = headers
        .iter()
        .position(|h| *h == column)
        .context(format!("Missing column {column}"))?;

    let total_column = headers
        .iter()
        .position(|h| *h == "Total")
        .context("Missing column Total")?;

    let data_lines = lines.skip(100);

    let mut total_sum = 0.0;
    let mut count = 0;

    for line in data_lines {
        let cols: Vec<&str> = line.split(',').collect();
        if let Some(value_str) = cols.get(total_column)
            && let Ok(value) = value_str.trim().parse::<f64>()
            && (value.is_infinite() || value.is_nan() || value <= 0.0 || value > 300.0)
        {
            continue;
        }

        if let Some(value_str) = cols.get(selected_column)
            && let Ok(value) = value_str.trim().parse::<f64>()
            && value.is_finite()
        {
            total_sum += value;
            count += 1;
        }
    }

    if count == 0 {
        bail!("No valid data points found")
    } else {
        Ok(total_sum / count as f64)
    }
}

#[derive(thiserror::Error, Debug)]
pub enum CommandError {
    #[error("Failed to launch command {0}")]
    LaunchError(#[from] std::io::Error),
    #[error("Command failed with {exit_code}")]
    RunError {
        exit_code: i32,
        stderr: String,
        stdout: String,
    },
    #[error("Failed to parse into UTF8 string {0}")]
    StringParseError(#[from] FromUtf8Error),
}

pub async fn simple_command_with_output(
    program: &str,
    args: &[&str],
    dir: &Path,
) -> Result<String, CommandError> {
    let output = Command::new(program)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .current_dir(dir)
        .output()
        .await
        .map_err(CommandError::LaunchError)?;
    if !output.status.success() {
        return Err(CommandError::RunError {
            exit_code: output.status.code().unwrap(),
            stderr: String::from_utf8(output.stderr).map_err(CommandError::StringParseError)?,
            stdout: String::from_utf8(output.stdout).map_err(CommandError::StringParseError)?,
        });
    }

    if output.stderr.len() > 0 {
        info!("stderr: {}", String::from_utf8(output.stderr).unwrap());
    }
    String::from_utf8(output.stdout).map_err(CommandError::StringParseError)
}

pub async fn simple_command_with_output_no_dir(
    program: &str,
    args: &[&str],
) -> Result<String, CommandError> {
    simple_command_with_output(program, args, &std::env::current_dir().unwrap()).await
}

#[derive(Debug, Default, Clone, PartialOrd, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Filesystem {
    #[default]
    None,
    Ext4,
    Xfs,
    Btrfs,
}

impl Filesystem {
    pub fn cmd(&self, device: &str) -> Result<String> {
        Ok(match self {
            Filesystem::None => bail!("No filesystem specified"),
            Filesystem::Ext4 => format!("sudo mkfs.ext4 -F -L ext4_bench {device}"),
            Filesystem::Xfs => format!("sudo mkfs.xfs -f -L xfs_bench {device}"),
            Filesystem::Btrfs => format!("sudo mkfs.btrfs -f -L btrfs_bench {device}"),
        })
    }

    pub fn ord(&self) -> usize {
        match self {
            Filesystem::None => 0,
            Filesystem::Ext4 => 1,
            Filesystem::Xfs => 2,
            Filesystem::Btrfs => 3,
        }
    }
}

pub async fn read_until_prompt(
    reader: &mut Pin<&mut impl AsyncReadExt>,
    prompt: &str,
) -> Result<String> {
    let prompt_bytes = prompt.as_bytes();
    let mut buffer = Vec::new();
    let mut window = Vec::new();
    loop {
        let mut byte = [0u8];
        let n = reader.read(&mut byte).await?;
        if n == 0 {
            break;
        }
        buffer.push(byte[0]);

        window.push(byte[0]);
        if window.len() > prompt_bytes.len() {
            window.remove(0);
        }
        if window.as_slice() == prompt_bytes {
            break;
        }
    }
    Ok(String::from_utf8_lossy(&buffer).into_owned())
}

#[derive(Debug, Deserialize)]
struct Marker {
    time: usize,
}

pub fn calculate_sectioned<CalculatedData: Debug + Default + Copy, const N: usize>(
    marker_csv: Option<&str>,
    csv_to_section: &str,
    column_name: &str,
    lower: f64,
    upper: f64,
    calculator: fn(data: &[(usize, f64)]) -> CalculatedData,
) -> Result<([CalculatedData; N], CalculatedData, [usize; N])> {
    let markers = match marker_csv {
        Some(marker_csv) => {
            let mut marker_reader = ReaderBuilder::new()
                .has_headers(true)
                .from_reader(marker_csv.as_bytes());
            let markers: Vec<Marker> = marker_reader.deserialize().collect::<Result<_, _>>()?;
            let markers = markers.into_iter().map(|x| x.time).collect::<Vec<_>>();
            if markers.len() != N {
                bail!("Expected {} markers, got {}", N, markers.len());
            }
            markers
        }
        None => vec![],
    };

    let mut rdr = ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv_to_section.as_bytes());
    let headers = rdr.headers()?.clone();

    let time_idx = headers
        .iter()
        .position(|h| h == "time")
        .context("No 'time' column found")?;
    let col_idx = headers
        .iter()
        .position(|h| h == column_name)
        .context(format!("No '{column_name}' column found"))?;

    let records: Vec<StringRecord> = rdr.records().filter_map(Result::ok).collect();
    let parse = |rec: &StringRecord| -> Option<(usize, f64)> {
        let time = rec.get(time_idx)?.parse().ok()?;
        let val: f64 = rec.get(col_idx)?.parse().ok()?;
        if val.is_nan() || !val.is_finite() {
            return None;
        }
        if val < lower || val > upper {
            return None;
        }
        Some((time, val))
    };

    let data: Vec<(usize, f64)> = records.iter().filter_map(parse).collect();
    let mut prev = 0;
    let mut stats = [CalculatedData::default(); N];
    let mut markers_final = [0; N];
    for (i, bound) in markers.iter().enumerate() {
        let section_data: Vec<(usize, f64)> = data
            .iter()
            .filter(|&&(t, _)| t >= prev && t < *bound)
            .cloned()
            .collect();

        prev = *bound;
        stats[i] = calculator(&section_data);
        markers_final[i] = *bound;
    }

    let overall = calculator(&data);

    Ok((stats, overall, markers_final))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceCalls {
    pub function: String,
    pub time: usize,
    pub count: usize,
    pub is_nvme_call: bool,
    pub has_fs_pagewrite: bool,
    pub requeued_io: bool,
    pub vfs_read: bool,
    pub vfs_write: bool,
    pub vfs_fsync: bool,
}

pub fn parse_trace(input: &str, fs: &Filesystem) -> Result<Vec<TraceCalls>> {
    let fs_writepage_query = match fs {
        Filesystem::None => unimplemented!(),
        Filesystem::Ext4 => "ext4_writepages",
        Filesystem::Xfs => "xfs_vm_writepages",
        Filesystem::Btrfs => "do_writepages",
    };
    let mut records = Vec::new();
    let mut current_ts: Option<usize> = None;
    let mut lines = input.lines().peekable();

    while let Some(raw) = lines.next() {
        let line = raw.trim_start();

        if let Some(ts_str) = line.strip_prefix("time:") {
            if let Some(tok) = ts_str.split_whitespace().next() {
                current_ts = tok.parse().ok();
            } else {
                current_ts = None;
            }
            continue;
        }

        if line.starts_with("@io_graph[") {
            let ts = match current_ts {
                Some(t) => t,
                None => continue,
            };

            let mut body_lines = Vec::new();
            let mut count = 0usize;

            if let Some(idx) = line.find("]: ") {
                let inside = &line[line.find('[').unwrap() + 1..idx];
                body_lines.push(inside.to_string());
                count = line[idx + 2..].trim().parse().unwrap_or(0);
            } else {
                for next_raw in lines.by_ref() {
                    let l = next_raw.trim();
                    if let Some(idx) = l.find("]: ") {
                        body_lines.push(l[..idx].to_string());
                        count = l[idx + 2..].trim().parse().unwrap_or(0);
                        break;
                    } else {
                        body_lines.push(l.to_string());
                    }
                }
            }

            let queries = [
                "nvme_",
                fs_writepage_query,
                "blk_mq_requeue_work",
                "vfs_read",
                "vfs_write",
                "vfs_fsync",
            ]
            .into_iter()
            .map(|x| body_lines.iter().any(|l| l.trim().starts_with(x)))
            .collect::<Vec<_>>();

            if let Some(root) = body_lines
                .into_iter()
                .map(|l| l.trim().to_string())
                .find(|l| !l.is_empty() && !l.starts_with("0x") && l != ",")
            {
                records.push(TraceCalls {
                    function: root,
                    time: ts,
                    count,
                    is_nvme_call: queries[0],
                    has_fs_pagewrite: queries[1],
                    requeued_io: queries[2],
                    vfs_read: queries[3],
                    vfs_write: queries[4],
                    vfs_fsync: queries[5],
                });
            }
        }
    }

    Ok(records)
}

pub fn write_csv<T: Serialize>(filename: &PathBuf, records: &[T]) -> Result<()> {
    let mut wtr = Writer::from_path(filename)?;
    for record in records {
        wtr.serialize(record)?;
    }
    wtr.flush()?;
    Ok(())
}

#[derive(Debug, Default, Copy, Clone)]
pub struct SectionStats {
    pub power: Option<f64>,
    pub energy: Option<f64>,
}

pub fn power_energy_calculator(data: &[(usize, f64)]) -> SectionStats {
    let (sum, count) = data.iter().fold((0.0, 0), |(s, c), &(_, v)| (s + v, c + 1));
    let mean = if count > 0 {
        Some(sum / count as f64)
    } else {
        None
    };

    let energy = if data.len() >= 2 {
        let mut e = 0.0;
        for win in data.windows(2) {
            let (t0, p0) = win[0];
            let (t1, p1) = win[1];
            let dt = (t1 as f64) - (t0 as f64);
            e += 0.5 * (p0 + p1) * dt;
        }
        Some(e)
    } else {
        None
    };

    SectionStats {
        power: mean,
        energy,
    }
}

pub async fn mount_fs(
    mountpoint: &Path,
    device: &str,
    fs: Filesystem,
    should_format: bool,
    mount_opts: Option<impl Into<String>>,
) -> Result<()> {
    create_dir_all(mountpoint).await?;
    if let Err(err) = simple_command_with_output_no_dir("umount", &[device]).await {
        match &err {
            CommandError::RunError { stderr, .. } => {
                if !stderr.contains(": not mounted.") {
                    bail!(err);
                }
            }
            _ => {
                bail!(err);
            }
        }
    }

    if should_format {
        _ = simple_command_with_output_no_dir("bash", &["-c", &fs.cmd(device)?]).await?;
    }
    let mut args = match mount_opts {
        Some(mount_opts) => vec!["-o".to_owned(), mount_opts.into()],
        None => vec![],
    };

    args.extend([device.to_owned(), mountpoint.to_str().unwrap().to_owned()]);
    _ = simple_command_with_output_no_dir(
        "mount",
        &args.iter().map(|x| x.as_str()).collect::<Vec<_>>(),
    )
    .await?;
    Ok(())
}

pub async fn chown_user(dir: &Path) -> Result<()> {
    _ = simple_command_with_output_no_dir(
        "chown",
        &[
            "-R",
            &std::env::var("SUDO_USER").context("energy-benchmark expectes to be run with sudo")?,
            dir.to_str().unwrap(),
        ],
    )
    .await;
    Ok(())
}
