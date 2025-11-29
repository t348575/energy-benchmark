use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    fs,
    hash::Hash,
    ops::AddAssign,
    path::{Path, PathBuf},
    pin::Pin,
    process::Stdio,
    string::FromUtf8Error,
    time::Instant,
};

use csv::{ReaderBuilder, StringRecord, Writer};
use eyre::{Context, ContextCompat, Result, bail};
use flume::{Receiver, Sender};
use rayon::{
    iter::{IntoParallelRefIterator, ParallelIterator},
    slice::ParallelSlice,
};
use regex::Regex;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::{
    fs::{
        File, OpenOptions, create_dir_all, read_dir, read_to_string as tokio_read_to_string,
        remove_dir_all, remove_file,
    },
    io::{self, AsyncReadExt, AsyncWriteExt},
    process::Command,
};
use tracing::{debug, error, info, warn};

use crate::{
    bench::BenchInfo,
    sensor::{SensorArgs, SensorReply, SensorRequest},
};

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
pub async fn sensor_reader<Args, Sensor, InitSensor, InitSensorFut, ReadSensorData, SensorData>(
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
    Sensor: Send + 'static,
    InitSensor: Fn(Args) -> InitSensorFut,
    InitSensorFut: Future<Output = Result<(Sensor, Vec<String>)>> + Send + 'static,
    ReadSensorData: for<'a> Fn(
        &'a Args,
        &'a mut Sensor,
        &SensorRequest,
        Instant,
    ) -> Pin<
        Box<dyn Future<Output = Result<SensorData, SensorError>> + Send + 'a>,
    >,
{
    debug!("Spawning {} reader", args.name());
    let args_copy = args.clone();
    let (mut s, sensor_names) = init(args_copy).await?;

    let mut readings = Vec::with_capacity(45_000);
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
                match read(&args, &mut s, &req, last_time).await {
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
                        let filename = dir.join(filename);
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

pub fn blocking_sensor_reader<Args, Sensor, InitSensor, ReadSensorData, SensorData>(
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
    Sensor: Send + 'static,
    InitSensor: Fn(Args) -> Result<(Sensor, Vec<String>)> + Send + 'static,
    ReadSensorData: for<'a> Fn(
        &'a Args,
        &'a mut Sensor,
        &SensorRequest,
        Instant,
    ) -> Result<SensorData, SensorError>,
{
    debug!("Spawning {} reader", args.name());
    let args_copy = args.clone();
    let (mut s, sensor_names) = init(args_copy)?;

    let mut readings = Vec::with_capacity(45_000);
    let mut is_running = false;
    let mut dir = PathBuf::new();
    let mut req = SensorRequest::StopRecording;
    let mut start_time = Instant::now();
    let mut read_time = Instant::now();
    let mut last_time = Instant::now();
    let mut error_count = 0;
    loop {
        if !is_running {
            if let Ok(request) = rx.recv() {
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
                match read(&args, &mut s, &req, last_time) {
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
                && let Ok(request) = rx.recv()
            {
                match request {
                    SensorRequest::StopRecording => {
                        use std::io::Write;
                        debug!("Stopping {} reader", args.name());
                        is_running = false;
                        let filename = dir.join(filename);
                        let mut file = std::fs::File::create(filename)?;

                        file.write_all(format!("time,{}\n", sensor_names.join(",")).as_bytes())?;
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
                            )?;
                        }
                        file.flush()?;
                        tx.send(SensorReply::FileDumpComplete)?;
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
    let only_process = std::env::var("ONLY_PROCESS").unwrap_or("0".to_owned());
    if only_process == "1" {
        return Ok(());
    }
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

pub async fn read_json_file<T>(path: impl AsRef<Path>) -> Result<T>
where
    T: DeserializeOwned,
{
    let data = tokio_read_to_string(path.as_ref()).await?;
    Ok(serde_json::from_str(&data)?)
}

#[derive(Serialize)]
struct BarChartSpec {
    data: Vec<Vec<f64>>,
    labels: Vec<String>,
    title: String,
    x_label: String,
    y_label: String,
    output_path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    legend_labels: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tick_rotation_deg: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tick_horizontal_align: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bar_width: Option<f64>,
    nvme_power_states: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct BarChartConfig {
    pub title: String,
    pub x_label: String,
    pub y_label: String,
    pub legend_labels: Option<Vec<String>>,
    pub tick_rotation_deg: Option<f64>,
    pub tick_horizontal_align: Option<String>,
    pub bar_width: Option<f64>,
    pub y_scale: Option<String>,
}

impl BarChartConfig {
    pub fn new(
        title: impl Into<String>,
        x_label: impl Into<String>,
        y_label: impl Into<String>,
    ) -> Self {
        Self {
            title: title.into(),
            x_label: x_label.into(),
            y_label: y_label.into(),
            legend_labels: None,
            tick_rotation_deg: None,
            tick_horizontal_align: None,
            bar_width: None,
            y_scale: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum BarChartKind {
    Throughput,
    Latency,
    Power,
    NormalizedPower,
    Freq,
    Load,
}

pub fn make_power_state_bar_config(
    kind: BarChartKind,
    x_label: &str,
    experiment_name: &str,
    name_prefix: Option<&str>,
) -> BarChartConfig {
    let clean_prefix = name_prefix.and_then(|p| {
        let trimmed = p.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    });

    match kind {
        BarChartKind::Throughput => {
            let title = format!("Throughput for {} vs. power state", x_label.to_lowercase());
            let mut config = BarChartConfig::new(
                title,
                format!("{} {}", x_label, experiment_name),
                "Throughput (MiB/s)",
            );
            config.tick_rotation_deg = Some(45.0);
            config.tick_horizontal_align = Some("right".to_owned());
            config
        }
        BarChartKind::Latency => {
            let title = match clean_prefix {
                Some(prefix) => format!(
                    "{} Latency for {} vs. power state",
                    prefix,
                    x_label.to_lowercase()
                ),
                None => format!("Latency for {} vs. power state", x_label.to_lowercase()),
            };
            BarChartConfig::new(
                title,
                format!("{} {}", x_label, experiment_name),
                "Latency (ms)",
            )
        }
        BarChartKind::Power => {
            let title = match clean_prefix {
                Some(prefix) => format!("{} power vs. {}", prefix, x_label.to_lowercase()),
                None => format!("Power vs. {}", x_label.to_lowercase()),
            };
            let mut config = BarChartConfig::new(title, x_label.to_owned(), "Power (Watts)");
            config.tick_rotation_deg = Some(45.0);
            config.tick_horizontal_align = Some("right".to_owned());
            config
        }
        BarChartKind::NormalizedPower => {
            let title = match clean_prefix {
                Some(prefix) => {
                    format!("{} power increase % vs. {}", prefix, x_label.to_lowercase())
                }
                None => format!("Power vs. {}", x_label.to_lowercase()),
            };
            let mut config = BarChartConfig::new(
                title,
                x_label.to_owned(),
                "Power (W) percentage increase (%)",
            );
            config.tick_rotation_deg = Some(45.0);
            config.tick_horizontal_align = Some("right".to_owned());
            config.y_scale = Some("not_from_zero".to_owned());
            config
        }
        BarChartKind::Freq => {
            let title = match clean_prefix {
                Some(prefix) => format!("{} frequency vs. {}", prefix, x_label.to_lowercase()),
                None => format!("Frequency vs. {}", x_label.to_lowercase()),
            };
            let mut config = BarChartConfig::new(title, x_label.to_owned(), "Frequency (MHz)");
            config.tick_rotation_deg = Some(45.0);
            config.tick_horizontal_align = Some("right".to_owned());
            config
        }
        BarChartKind::Load => {
            let title = match clean_prefix {
                Some(prefix) => format!("{} load vs. {}", prefix, x_label.to_lowercase()),
                None => format!("Load vs. {}", x_label.to_lowercase()),
            };
            let mut config = BarChartConfig::new(title, x_label.to_owned(), "Load");
            config.tick_rotation_deg = Some(45.0);
            config.tick_horizontal_align = Some("right".to_owned());
            config
        }
    }
}

pub fn plot_bar_chart(
    filepath: &Path,
    data: Vec<Vec<f64>>,
    labels: Vec<String>,
    config: BarChartConfig,
    bench_info: &BenchInfo,
) -> Result<()> {
    if data.is_empty() {
        return Ok(());
    }

    let parent = filepath
        .parent()
        .context("Bar chart output path missing parent directory")?;
    let plot_data_dir = parent.join("plot_data");
    if !plot_data_dir.exists() {
        fs::create_dir_all(&plot_data_dir)?;
    }

    let stem = filepath
        .file_stem()
        .and_then(|s| s.to_str())
        .context("Failed to derive bar chart file stem")?;
    let spec_path = plot_data_dir.join(format!("{stem}.bar.json"));

    let output_path = filepath
        .to_str()
        .context("Bar chart output path is not valid UTF-8")?
        .to_owned();

    let spec = BarChartSpec {
        data,
        labels,
        title: config.title,
        x_label: config.x_label,
        y_label: config.y_label,
        output_path,
        legend_labels: config.legend_labels,
        tick_rotation_deg: config.tick_rotation_deg,
        tick_horizontal_align: config.tick_horizontal_align,
        bar_width: config.bar_width,
        nvme_power_states: bench_info
            .device_power_states
            .iter()
            .map(|x| x.1.clone())
            .collect(),
    };

    let spec_serialized = serde_json::to_string(&spec)?;
    fs::write(&spec_path, spec_serialized)?;

    let spec_path_str = spec_path
        .to_str()
        .context("Bar chart spec path is not valid UTF-8")?
        .to_owned();
    let args = vec![("--spec".to_owned(), spec_path_str)];
    plot_python("bar_chart", &args)
}

fn sanitize_filename(input: &str) -> String {
    input
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' => c,
            _ => '_',
        })
        .collect()
}

#[derive(Debug, Clone, Serialize)]
pub struct TimeSeriesPlot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub x_axis: Option<TimeSeriesAxis>,
    pub y_axis: Vec<TimeSeriesAxis>,
    pub time: TimeSeriesAxis,
    #[serde(default)]
    pub secondary_y_axis: Vec<TimeSeriesAxis>,
    pub title: String,
    pub file_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dir: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimeSeriesAxis {
    pub axis_type: TimeSeriesAxisType,
    pub dataset_name: String,
    pub dataset_field: String,
    pub plot_label: String,
    pub axis_label: String,
}

impl TimeSeriesAxis {
    pub fn sensor(
        sensor: impl Into<String>,
        dataset_field: impl Into<String>,
        plot_label: impl Into<String>,
        axis_label: impl Into<String>,
    ) -> Self {
        Self {
            axis_type: TimeSeriesAxisType::Sensor,
            dataset_name: sensor.into(),
            dataset_field: dataset_field.into(),
            plot_label: plot_label.into(),
            axis_label: axis_label.into(),
        }
    }

    pub fn bench(
        dataset_field: impl Into<String>,
        plot_label: impl Into<String>,
        axis_label: impl Into<String>,
    ) -> Self {
        Self {
            axis_type: TimeSeriesAxisType::Bench,
            dataset_name: "bench".into(),
            dataset_field: dataset_field.into(),
            plot_label: plot_label.into(),
            axis_label: axis_label.into(),
        }
    }

    pub fn sensor_time(sensor: impl Into<String>) -> Self {
        Self::sensor(sensor, "time", "Time", "Time (s)")
    }

    pub fn bench_time() -> Self {
        Self::bench("time", "Time", "Time (s)")
    }
}

impl TimeSeriesPlot {
    pub fn new(
        dir: Option<String>,
        file_name: impl Into<String>,
        title: impl Into<String>,
        time: TimeSeriesAxis,
        y_axis: Vec<TimeSeriesAxis>,
    ) -> Self {
        Self {
            x_axis: None,
            y_axis,
            time,
            secondary_y_axis: Vec::new(),
            title: title.into(),
            file_name: file_name.into(),
            dir,
        }
    }

    pub fn with_secondary(mut self, axes: Vec<TimeSeriesAxis>) -> Self {
        self.secondary_y_axis = axes;
        self
    }

    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = title.into();
        self
    }

    pub fn with_filename(mut self, file_name: impl Into<String>) -> Self {
        self.file_name = file_name.into();
        self
    }
}

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeSeriesAxisType {
    Sensor,
    Bench,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimeSeriesSpec {
    pub bench_type: String,
    pub plot_dir: PathBuf,
    pub results_dir: PathBuf,
    pub config_yaml: PathBuf,
    pub info_json: PathBuf,
    pub name: String,
    pub plots: Vec<TimeSeriesPlot>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offset: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trim_end: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<usize>,
}

impl TimeSeriesSpec {
    pub fn new(
        bench_type: impl Into<String>,
        plot_dir: impl Into<PathBuf>,
        results_dir: impl Into<PathBuf>,
        name: impl Into<String>,
        plots: Vec<TimeSeriesPlot>,
    ) -> Self {
        let name = name.into();
        let results_dir = results_dir.into();
        let base_dir: PathBuf = results_dir
            .clone()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .into();

        Self {
            bench_type: bench_type.into(),
            plot_dir: plot_dir.into(),
            results_dir,
            config_yaml: base_dir.join("config.yaml"),
            info_json: base_dir.join("info.json"),
            name,
            plots,
            offset: None,
            trim_end: None,
            width: None,
        }
    }

    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset.replace(offset);
        self
    }

    pub fn with_trim_end(mut self, trim_end: usize) -> Self {
        self.trim_end.replace(trim_end);
        self
    }

    pub fn with_width(mut self, width: usize) -> Self {
        self.width.replace(width);
        self
    }

    pub fn with_plots(mut self, plots: Vec<TimeSeriesPlot>) -> Self {
        self.plots = plots;
        self
    }

    fn plot_dir(&self) -> &Path {
        &self.plot_dir
    }

    fn name(&self) -> &str {
        &self.name
    }
}

pub fn plot_time_series(spec: TimeSeriesSpec) -> Result<()> {
    let plot_dir = spec.plot_dir();
    if !plot_dir.exists() {
        fs::create_dir_all(plot_dir)?;
    }

    let mut ensured_dirs = HashSet::new();
    for plot in &spec.plots {
        match &plot.dir {
            Some(dir) => {
                if ensured_dirs.insert(dir.to_owned()) {
                    fs::create_dir_all(plot_dir.join(dir))?;
                }
            }
            None => continue,
        }
    }

    let spec_dir = plot_dir.join("plot_specs");
    if !spec_dir.exists() {
        fs::create_dir_all(&spec_dir)?;
    }

    let spec_filename = format!("{}.time.json", sanitize_filename(spec.name()));
    let spec_path = spec_dir.join(spec_filename);

    let spec_serialized = serde_json::to_string(&spec)?;
    fs::write(&spec_path, spec_serialized)?;

    let spec_path_str = spec_path
        .to_str()
        .context("Time-series spec path is not valid UTF-8")?
        .to_owned();
    let args = vec![("--spec".to_owned(), spec_path_str)];
    plot_python("time_series", &args)
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

    if !output.stderr.is_empty() {
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
#[serde(deny_unknown_fields)]
pub enum Filesystem {
    #[default]
    None,
    Ext4,
    Xfs,
    Btrfs,
    F2fs,
}

impl Filesystem {
    pub fn cmd(&self, device: &str) -> Result<String> {
        Ok(match self {
            Filesystem::None => bail!("No filesystem specified"),
            Filesystem::Ext4 => format!("sudo mkfs.ext4 -F -L ext4_bench {device}"),
            Filesystem::Xfs => format!("sudo mkfs.xfs -f -L xfs_bench {device}"),
            Filesystem::Btrfs => format!("sudo mkfs.btrfs -f -L btrfs_bench {device}"),
            Filesystem::F2fs => format!("sudo mkfs.f2fs -f -l f2fs_bench {device}"),
        })
    }

    pub fn ord(&self) -> usize {
        match self {
            Filesystem::None => 0,
            Filesystem::Ext4 => 1,
            Filesystem::Xfs => 2,
            Filesystem::Btrfs => 3,
            Filesystem::F2fs => 4,
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
) -> Result<([CalculatedData; N], CalculatedData, [usize; N])> {
    assert_eq!(columns.len(), limits.len());
    let markers = match marker_csv {
        Some(marker_csv) => {
            let mut marker_reader = ReaderBuilder::new()
                .has_headers(true)
                .from_reader(marker_csv.as_bytes());
            let markers: Vec<Marker> = marker_reader.deserialize().collect::<Result<_, _>>()?;
            let markers = markers.into_iter().map(|x| x.time).collect::<Vec<_>>();
            if markers.len() != N - 1 {
                bail!("Expected {} markers, got {}", N - 1, markers.len());
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
            columns
                .iter()
                .position(|c| Regex::new(c).unwrap().is_match_at(col, 0))
                .map(|col_filter_idx| (col_filter_idx, idx))
        })
        .collect::<Vec<_>>();

    if col_indexes.len() < columns.len() {
        bail!(
            "Expected {} columns, got {}. A specified column does not exist in the csv",
            columns.len(),
            col_indexes.len()
        );
    }

    let time_idx = headers.iter().position(|h| h == "time").unwrap();

    let parse = |rec: &StringRecord| -> Option<(usize, Vec<f64>)> {
        let time = rec.get(time_idx)?.parse().ok()?;
        let values = col_indexes
            .iter()
            .filter_map(|col_idx| {
                let val = rec.get(col_idx.1)?;
                let val = val.parse::<f64>().ok()?;
                if val.is_nan()
                    || !val.is_finite()
                    || (val < limits[col_idx.0].0 || val > limits[col_idx.0].1)
                {
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

    if let Some(last_marker) = markers.last() {
        let tail_data: Vec<_> = data
            .iter()
            .filter(|&&(t, _)| t >= *last_marker)
            .cloned()
            .collect();

        stats[N - 1] = calculator(&tail_data);
        markers_final[N - 1] = data.last().map(|(t, _)| *t).unwrap_or(prev);
    }

    let overall = calculator(&data);
    Ok((stats, overall, markers_final))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
        Filesystem::F2fs => "f2fs_writepages", // TODO: check if this is correct
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
    pub power_mean: Option<f64>,
    pub power_stddev: Option<f64>,
    pub energy: Option<f64>,
}

pub fn power_energy_calculator(data: &[(usize, Vec<f64>)]) -> SectionStats {
    let count = data.len();
    let sum = data
        .par_iter()
        .map(|(_, v)| v.iter().sum::<f64>())
        .sum::<f64>();
    let mean = if count > 0 {
        Some(sum / count as f64)
    } else {
        None
    };

    let stddev = if let Some(mean) = mean {
        let variance = data
            .par_iter()
            .map(|(_, v)| {
                v.iter()
                    .map(|x| {
                        let diff = *x - mean;
                        diff * diff
                    })
                    .sum::<f64>()
            })
            .sum::<f64>()
            / count as f64;
        Some(variance.sqrt())
    } else {
        None
    };

    let energy = if count >= 2 {
        Some(
            data.par_windows(2)
                .map(|win| {
                    let (t0, p0) = &win[0];
                    let (t1, p1) = &win[1];
                    let dt = (*t1 as f64) - (*t0 as f64);
                    0.5 * (p0[0] + p1[0]) * (dt / 1000.0)
                })
                .sum::<f64>(),
        )
    } else {
        None
    };

    SectionStats {
        power_mean: mean,
        power_stddev: stddev,
        energy,
    }
}

pub fn sysinfo_average_calculator(data: &[(usize, Vec<f64>)]) -> (f64, f64) {
    let (sum_freq, sum_load, n) = data
        .par_iter()
        .map(|(_, v)| {
            let half = v.len() / 2;
            let freq = v[..half].iter().sum::<f64>() / half as f64;
            let load = v[half..].iter().sum::<f64>() / half as f64;
            (freq, load, 1usize)
        })
        .reduce(
            || (0.0, 0.0, 0usize),
            |(f1, l1, c1), (f2, l2, c2)| (f1 + f2, l1 + l2, c1 + c2),
        );

    (sum_freq / n as f64, sum_load / n as f64)
}

pub async fn mount_fs(
    mountpoint: &Path,
    device: &str,
    fs: &Filesystem,
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
        if let Some(file_name) = component.file_name()
            && let Some(s) = file_name.to_str()
            && re.is_match(s)
        {
            return Some(s.to_string());
        }
    }
    None
}

pub async fn get_cpu_topology() -> Result<HashMap<u32, u32>> {
    let mut topology = HashMap::new();
    let mut dir = tokio::fs::read_dir("/sys/devices/system/cpu/").await?;
    while let Some(entry) = dir.next_entry().await? {
        let file_name = entry.file_name();
        let file_name_str = file_name.to_string_lossy();

        if !file_name_str.starts_with("cpu") {
            continue;
        }
        let cpu_index_str = &file_name_str[3..];
        if cpu_index_str.is_empty() || !cpu_index_str.chars().all(|c| c.is_ascii_digit()) {
            continue;
        }

        let topology_path = entry.path().join("topology/physical_package_id");
        if Path::new(&topology_path).exists() {
            let mut file = File::open(topology_path).await?;
            let mut result = String::new();
            file.read_to_string(&mut result).await?;
            if let Ok(package_id) = result.trim().parse::<u32>() {
                topology.entry(package_id).or_insert(0).add_assign(1);
            }
        }
    }
    Ok(topology)
}

pub async fn write_one_line<P: AsRef<Path>>(path: P, s: &str) -> io::Result<()> {
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .open(&path)
        .await?;
    _ = f.write(format!("{s}\n").as_bytes()).await?;
    Ok(())
}

pub async fn clean_directory_except_prefill<P: AsRef<Path>>(dir: P) -> io::Result<()> {
    let mut entries = read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();

        if path.file_name().is_some_and(|name| name == "prefill") {
            continue;
        }

        let metadata = entry.metadata().await?;
        if metadata.is_dir() {
            remove_dir_all(&path).await?;
        } else {
            remove_file(&path).await?;
        }
    }

    Ok(())
}
