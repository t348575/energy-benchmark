use std::{collections::HashSet, hash::Hash, path::PathBuf};

use eyre::{Context, ContextCompat, Result, bail};
use flume::{Receiver, Sender};
use pyo3::{
    Bound, PyAny, PyResult, Python,
    types::{PyAnyMethods, PyListMethods},
};
use tokio::{fs::File, io::AsyncWriteExt, task::spawn_blocking};
use tracing::{debug, error, warn};

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

pub fn parse_request_size(request_size: &str) -> Result<u64> {
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
pub async fn simple_sensor_reader<Args, Sensor, ReadSensorData, Fut, SensorData>(
    rx: Receiver<SensorRequest>,
    tx: Sender<SensorReply>,
    filename: &str,
    args: Args,
    init: fn(&Args) -> Result<(Sensor, Vec<String>)>,
    read: ReadSensorData,
) -> Result<()>
where
    Args: SensorArgs + Clone,
    SensorData: IntoIterator,
    SensorData::Item: ToString,
    Sensor: Clone + Send + 'static,
    ReadSensorData: Fn(&Args, &Sensor, &SensorRequest) -> Fut,
    Fut: Future<Output = Result<SensorData>>,
{
    let args_copy = args.clone();
    let (s, sensor_names) = spawn_blocking(move || init(&args_copy)).await??;
    debug!("Spawning {} reader", args.name());

    let mut readings = Vec::new();
    let mut is_running = false;
    let mut dir = PathBuf::new();
    let mut req = SensorRequest::StopRecording;
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
                        }
                    }
                    SensorRequest::Quit => break,
                    SensorRequest::StopRecording => {
                        is_running = false;
                        warn!("Expected {} start request, got stop instead", args.name());
                    }
                }
            }
        } else {
            match read(&args, &s, &req).await {
                Ok(t) => readings.push(t),
                Err(err) => error!(
                    "Error collecting sensor data for {} {:#?}",
                    args.name(),
                    err
                ),
            }

            if !rx.is_empty() {
                if let Ok(request) = rx.recv_async().await {
                    if let SensorRequest::StopRecording = request {
                        debug!("Stopping {} reader", args.name());
                        is_running = false;
                        let filename = dir.join(format!("{filename}.csv"));
                        let mut file = File::create(filename).await?;

                        file.write_all(format!("{}\n", sensor_names.join(",")).as_bytes())
                            .await?;
                        for row in readings.drain(..) {
                            file.write_all(
                                format!(
                                    "{}\n",
                                    row.into_iter()
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
                    } else {
                        warn!(
                            "Expected sensor stop request for {}, got start instead",
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

pub fn plot_python<Func>(func: Func, plotting_file: &str) -> Result<()>
where
    Func: FnOnce(Python<'_>, Bound<'_, PyAny>) -> PyResult<()>,
{
    let result: PyResult<()> = Python::with_gil(|py| {
        let sys = py.import("sys")?;
        let path = sys.getattr("path")?;
        let path: &Bound<_> = path.downcast()?;
        path.insert(0, "plots")?;

        let user_module = py.import(plotting_file)?;

        func(py, user_module.getattr("gen_plots")?)
    });
    result?;
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
        if let Some(value_str) = cols.get(total_column) {
            if let Ok(value) = value_str.trim().parse::<f64>() {
                if value.is_infinite() || value.is_nan() || value <= 0.0 || value > 300.0 {
                    continue;
                }
            }
        }

        if let Some(value_str) = cols.get(selected_column) {
            if let Ok(value) = value_str.trim().parse::<f64>() {
                if value.is_finite() {
                    total_sum += value;
                    count += 1;
                }
            }
        }
    }

    if count == 0 {
        bail!("No valid data points found")
    } else {
        Ok(total_sum / count as f64)
    }
}
