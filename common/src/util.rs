use std::{
    collections::{HashMap, HashSet},
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
use regex::Regex;
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

#[derive(thiserror::Error, Debug)]
pub enum SensorError {
    #[error("Failed to read sensor data: {0}")]
    MajorFailure(eyre::Error),
    #[error("No changes to data since last call")]
    NoChanges,
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
    SensorData: IntoIterator + Debug,
    SensorData::Item: ToString,
    Sensor: Clone + Send + 'static,
    InitSensor: Fn(Args) -> InitSensorFut,
    InitSensorFut: Future<Output = Result<(Sensor, Vec<String>)>> + Send + 'static,
    ReadSensorData: Fn(&Args, &Sensor, &SensorRequest, Instant) -> ReadSensorFut,
    ReadSensorFut: Future<Output = Result<SensorData, SensorError>>,
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
    let mut error_count = 0;
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
                        error_count = 0;

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
            if error_count < 500 {
                match read(&args, &s, &req, last_time).await {
                    Ok(t) => readings.push((start_time.elapsed().as_millis(), t)),
                    Err(err) => match err {
                        SensorError::MajorFailure(err) => {
                            error_count += 1;
                            error!(
                                "Error collecting sensor data for {} {:#?}",
                                args.name(),
                                err
                            )
                        }
                        SensorError::NoChanges => {}
                    },
                }
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

pub fn plot_python<V>(plot_file: impl AsRef<str>, args: &[(V, V)]) -> Result<()>
where
    V: AsRef<str>,
{
    debug!(
        "python plots/{}.py {}",
        plot_file.as_ref(),
        args.iter()
            .flat_map(|x| [x.0.as_ref(), x.1.as_ref()])
            .collect::<Vec<_>>()
            .join(" ")
    );
    let skip_plot = std::env::var("SKIP_PLOT").unwrap_or("0".to_owned());
    if skip_plot == "1" || skip_plot.to_lowercase() == "true" {
        return Ok(());
    }

    let mut child = std::process::Command::new("python3")
        .arg(format!("plots/{}.py", plot_file.as_ref()))
        .args(
            args.iter()
                .flat_map(|(k, v)| [k.as_ref(), v.as_ref()])
                .collect::<Vec<_>>(),
        )
        .spawn()?;
    child.wait()?;
    Ok(())
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
    env: &HashMap<String, String>,
) -> Result<String, CommandError> {
    let output = Command::new(program)
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .current_dir(dir)
        .envs(env)
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
    simple_command_with_output(
        program,
        args,
        &std::env::current_dir().unwrap(),
        &HashMap::new(),
    )
    .await
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

/// `runtime` Required for fallback to old csv format, in milliseconds
pub fn calculate_sectioned<CalculatedData: Debug + Default + Copy, const N: usize>(
    marker_csv: Option<&str>,
    csv_to_section: &str,
    columns: &[&str],
    limits: &[(f64, f64)],
    calculator: fn(data: &[(usize, Vec<f64>)]) -> CalculatedData,
    runtime: Option<usize>,
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

    let records: Vec<StringRecord> = rdr.records().filter_map(Result::ok).collect();
    let col_indexes = headers
        .iter()
        .enumerate()
        .filter_map(|(idx, col)| {
            if let Some(col_filter_idx) = columns
                .iter()
                .position(|c| Regex::new(c).unwrap().is_match_at(col, 0))
            {
                Some((col_filter_idx, idx))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    if col_indexes.len() < columns.len() {
        bail!(
            "Expected {} columns, got {}. A specified column does not exist in the csv",
            columns.len(),
            col_indexes.len()
        );
    }

    let time_idx = headers.iter().position(|h| h == "time");
    if time_idx.is_none() {
        assert_eq!(N, 0);
        assert!(runtime.is_some());
        return old_csv_format(col_indexes, limits, runtime.unwrap(), calculator, records);
    }
    let time_idx = time_idx.unwrap();

    let parse = |rec: &StringRecord| -> Option<(usize, Vec<f64>)> {
        let time = rec.get(time_idx)?.parse().ok()?;
        let values = col_indexes
            .iter()
            .filter_map(|col_idx| {
                let val = rec.get(col_idx.1)?;
                let val = val.parse::<f64>().ok()?;
                if val.is_nan() || !val.is_finite() {
                    None
                } else if val < limits[col_idx.0].0 || val > limits[col_idx.0].1 {
                    None
                } else {
                    Some(val)
                }
            })
            .collect::<Vec<f64>>();
        if values.len() < columns.len() {
            return None;
        }

        Some((time, values))
    };

    let data: Vec<(usize, Vec<f64>)> = records.iter().filter_map(parse).collect();
    let mut prev = 0;
    let mut stats = [CalculatedData::default(); N];
    let mut markers_final = [0; N];
    for (i, bound) in markers.iter().enumerate() {
        let section_data: Vec<(usize, Vec<f64>)> = data
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

pub fn parse_trace<R: std::io::Read>(reader: R, fs: &Filesystem) -> Result<Vec<TraceCalls>> {
    let fs_writepage_query = match fs {
        Filesystem::None => unimplemented!(),
        Filesystem::Ext4 => "ext4_writepages",
        Filesystem::Xfs => "xfs_vm_writepages",
        Filesystem::Btrfs => "do_writepages",
    };

    let mut records = Vec::new();
    let mut current_ts: Option<usize> = None;
    let mut lines = std::io::BufRead::lines(std::io::BufReader::new(reader)).peekable();

    while let Some(line) = lines.next() {
        let line = line?;
        let line = line.trim_start();

        if let Some(ts_str) = line.strip_prefix("time:") {
            current_ts = ts_str
                .split_whitespace()
                .next()
                .and_then(|tok| tok.parse().ok());
            continue;
        }

        if line.starts_with("@io_graph[") {
            let ts = match current_ts {
                Some(t) => t,
                None => continue,
            };

            let mut body_lines = Vec::new();
            let mut count = 0usize;

            loop {
                let this_line = if body_lines.is_empty() {
                    let idx = line.find('[').unwrap() + 1;
                    line[idx..].to_owned()
                } else {
                    match lines.next() {
                        Some(Ok(l)) => l,
                        _ => break,
                    }
                };

                if let Some(idx) = this_line.find("]: ") {
                    body_lines.push(this_line[..idx].to_string());
                    count = this_line[idx + 3..].trim().parse().unwrap_or(0);
                    break;
                } else {
                    body_lines.push(this_line.to_string());
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
            .iter()
            .map(|x| body_lines.iter().any(|l| l.trim_start().starts_with(x)))
            .collect::<Vec<_>>();

            if let Some(root) = body_lines
                .iter()
                .map(|l| l.trim())
                .find(|l| !l.is_empty() && !l.starts_with("0x") && *l != ",")
            {
                records.push(TraceCalls {
                    function: root.to_string(),
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

pub fn power_energy_calculator(data: &[(usize, Vec<f64>)]) -> SectionStats {
    let (sum, count) = data
        .iter()
        .fold((0.0, 0), |(s, c), (_, v)| (s + v[0], c + 1));
    let mean = if count > 0 {
        Some(sum / count as f64)
    } else {
        None
    };

    let energy = if data.len() >= 2 {
        let mut e = 0.0;
        for win in data.windows(2) {
            let (t0, p0) = &win[0];
            let (t1, p1) = &win[1];
            let dt = (*t1 as f64) - (*t0 as f64);
            e += 0.5 * (p0[0] + p1[0]) * (dt / 1000.0);
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

pub fn sysinfo_average_calculator(data: &[(usize, Vec<f64>)]) -> (f64, f64) {
    let (sum, count) = data.iter().fold(((0.0, 0.0), 0), |(s, c), (_, v)| {
        let quarter = v.len() / 4;
        let half = v.len() / 2;
        let count = half - quarter;
        let freq = v[quarter..half].iter().sum::<f64>() / count as f64;
        let load = v[quarter + half..].iter().sum::<f64>() / count as f64;
        ((s.0 + freq, s.1 + load), c + 1)
    });
    (sum.0 / count as f64, sum.1 / count as f64)
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

/// Returns time in milliseconds
pub fn parse_time(time: &str) -> Result<usize> {
    let re = Regex::new(r"^(\d+)([smh])$").ok().unwrap();
    let caps = re.captures(time).context("Invalid time format")?;

    let value: usize = caps.get(1).unwrap().as_str().parse().ok().unwrap();
    let unit = caps.get(2).unwrap().as_str();

    Ok(match unit {
        "s" => value * 1000,
        "m" => value * 60 * 1000,
        "h" => value * 60 * 60 * 1000,
        _ => value * 1000,
    })
}

pub fn get_pcie_address(dev: &str) -> Option<String> {
    let dev_name = dev.trim_start_matches("/dev/");
    let sys_block = format!("/sys/block/{}", dev_name);
    let resolved = std::fs::read_link(&sys_block).ok()?;
    let abs_path = if resolved.is_absolute() {
        resolved
    } else {
        Path::new("/sys/block").join(resolved)
    };

    let re = Regex::new(r"^[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-9]$").unwrap();
    for component in abs_path.ancestors() {
        if let Some(file_name) = component.file_name() {
            if let Some(s) = file_name.to_str() {
                if re.is_match(s) {
                    return Some(s.to_string());
                }
            }
        }
    }
    None
}

fn old_csv_format<CalculatedData: Debug + Default + Copy, const N: usize>(
    col_indexes: Vec<(usize, usize)>,
    limits: &[(f64, f64)],
    runtime: usize,
    calculator: fn(data: &[(usize, Vec<f64>)]) -> CalculatedData,
    records: Vec<StringRecord>,
) -> Result<([CalculatedData; N], CalculatedData, [usize; N])> {
    let spacing = runtime as f64 / records.len() as f64;
    let parse = |item: (usize, &StringRecord)| -> Option<(usize, Vec<f64>)> {
        let (row_idx, rec) = item;
        let values = col_indexes
            .iter()
            .filter_map(|col_idx| {
                let val = rec.get(col_idx.1)?;
                let val = val.parse::<f64>().ok()?;
                if val.is_nan() || !val.is_finite() {
                    None
                } else if val < limits[col_idx.0].0 || val > limits[col_idx.0].1 {
                    None
                } else {
                    Some(val)
                }
            })
            .collect::<Vec<f64>>();

        if values.len() < col_indexes.len() {
            return None;
        }

        Some(((row_idx as f64 * spacing).round() as usize, values))
    };

    let data: Vec<(usize, Vec<f64>)> = records.iter().enumerate().filter_map(parse).collect();
    let overall = calculator(&data);
    Ok(([CalculatedData::default(); N], overall, [0; N]))
}
