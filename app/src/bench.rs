use std::{
    collections::HashMap,
    path::PathBuf,
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Local;
use common::{
    bench::{Bench, BenchArgs, BenchmarkInfo, Cmd},
    config::Config,
    plot::{PlotType, plot},
    sensor::{Sensor, SensorArgs, SensorRequest},
    util::remove_indices,
};
use console::style;
use eyre::{Context, Result, bail};
use flume::unbounded;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::{
    fs::{copy, create_dir_all, read_to_string, remove_dir_all},
    process::Command,
    spawn,
    sync::Mutex,
    time::sleep,
};
use tracing::{debug, error, info, warn};

pub async fn run_benchmark(config_file: String) -> Result<()> {
    let config: Config = serde_yml::from_str(&read_to_string(&config_file).await?)?;

    let total_units = calculate_total_units(&config);
    let pb = ProgressBar::new(total_units);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{wide_bar} {pos}/{len} | ETA: {msg}")
            .unwrap(),
    );
    let tracker = Arc::new(Mutex::new(TimingTracker::new(total_units)));
    let pb_c = pb.clone();
    let t_c = tracker.clone();
    spawn(async move {
        while !pb_c.is_finished() {
            {
                pb_c.set_message(t_c.lock().await.eta());
            }
            sleep(Duration::from_millis(1000)).await;
        }
    });

    let sensor_objects = default_sensors::SENSORS.get().unwrap().lock().await;
    let mut sensors = Vec::new();
    let mut sensor_replies = Vec::new();
    let mut loaded_sensors = Vec::new();
    let mut sensor_handles = Vec::new();

    for s in &config.sensors {
        if let Some(obj) = sensor_objects.iter().find(|s_obj| s_obj.name() == s) {
            let sensor_args = get_sensor_args(&config.sensor_args, obj.as_ref())?;
            let (req_tx, req_rx) = unbounded();
            let (resp_tx, resp_rx) = unbounded();
            sensor_handles.push(obj.start(&*sensor_args, req_rx, resp_tx)?);
            sensors.push(req_tx);
            sensor_replies.push(resp_rx);
            loaded_sensors.push(s);
        }
    }
    drop(sensor_objects);

    debug!("Loaded sensors: {loaded_sensors:?}");

    let file_prefix = Local::now().format("%Y-%m-%d_%H-%M-%S").to_string();
    println!(
        "Results created in folder: results/{}-{file_prefix}",
        config.name
    );
    let results_path = PathBuf::from("results").join(format!("{}-{file_prefix}", config.name));
    let data_path = results_path.join("data");
    create_dir_all(&results_path).await?;
    let plot_path = results_path.join("plots");
    _ = remove_dir_all(&plot_path).await;
    create_dir_all(&plot_path).await?;
    copy(config_file, results_path.join("config.yaml")).await?;

    let nvme_power_states = match &config.settings.nvme_power_states {
        Some(ps) => ps.iter().map(|x| *x as i32).collect(),
        None => vec![],
    };

    let s = sensors.clone();
    spawn(async move {
        tokio::signal::ctrl_c().await.unwrap();
        info!("Got CTRL+C signal");
        for s in s {
            s.send_async(SensorRequest::Quit).await.unwrap();
        }

        std::process::exit(0);
    });

    let nvme_cli_device = config
        .settings
        .nvme_cli_device
        .clone()
        .unwrap_or(config.settings.device.clone());

    let mut benchmark_info = HashMap::new();
    let total_experiments = config.benches.len();
    let mut current_experiment = 0;
    let total_power_states = nvme_power_states.len();

    for experiment in &config.benches {
        current_experiment += 1;
        let mut ps = nvme_power_states.clone();
        if ps.is_empty() {
            ps.push(-1);
        }

        let mut experiment_dirs = Vec::new();
        for power_state in ps {
            if power_state != -1 {
                let mut ps_change_cmd = Command::new("nvme")
                    .args([
                        "set-feature",
                        &nvme_cli_device,
                        "-f",
                        "2",
                        "--value",
                        &power_state.to_string(),
                    ])
                    .stdout(Stdio::null())
                    .stderr(Stdio::null())
                    .spawn()
                    .context("Set nvme power state")?;
                let status = ps_change_cmd.wait().await?;

                if !status.success() {
                    bail!("Could not change device power state to {power_state}");
                }
                info!("Power state of {nvme_cli_device} change to {power_state}");
            }

            let bench_args = get_bench_args(&config.bench_args, &*experiment.bench)?;
            let (program, cmds) =
                experiment
                    .bench
                    .cmds(&config.settings, &*bench_args, &experiment.name)?;

            let total_commands = cmds.len();
            for (
                current_command,
                Cmd {
                    args,
                    hash,
                    arg_obj,
                },
            ) in cmds.into_iter().enumerate()
            {
                let mut i = 0;
                let mut total_outliers = 0;
                let mut dirs = Vec::new();
                loop {
                    pb.set_message(format!(
                        "Experiment {}/{} | PS {}/{} | Cmd {}/{}: Iteration {}{}",
                        current_experiment,
                        total_experiments,
                        if power_state == -1 {
                            "N/A".to_owned()
                        } else {
                            power_state.to_string()
                        },
                        total_power_states,
                        current_command,
                        total_commands,
                        i,
                        if total_outliers > 0 {
                            format!(" ({} retries)", total_outliers)
                        } else {
                            String::new()
                        }
                    ));

                    let folder_name = format!(
                        "{}-ps{}-i{}-{}",
                        experiment.name,
                        power_state,
                        i,
                        hash.clone()
                    );
                    let final_path = data_path.join(&folder_name);
                    dirs.push(folder_name.clone());
                    benchmark_info.insert(
                        folder_name,
                        BenchmarkInfo {
                            args: arg_obj.clone(),
                            power_state,
                            iteration: i,
                            name: experiment.name.clone(),
                            hash: hash.clone(),
                        },
                    );
                    create_dir_all(&final_path).await?;

                    let mut args = args.clone();
                    experiment.bench.add_path_args(&mut args, &final_path);
                    debug!("iter={} program={} args={}", i, program, args.join(" "));
                    let result = experiment
                        .bench
                        .run(&program, &args, &sensors, &final_path, arg_obj.clone())
                        .await;

                    if let Err(err) = result {
                        error!("Failed to run benchmark: {err}");

                        for s in &sensors {
                            s.send_async(SensorRequest::Quit).await?;
                        }

                        return Err(err);
                    }

                    for s in &sensor_replies {
                        _ = s.recv_async().await?;
                    }
                    tracker.lock().await.increment();
                    pb.inc(1);

                    pb.set_message(format!(
                        "Iteration {} [{} retries]",
                        i + 1,
                        if total_outliers > 0 {
                            format!(" ({} retries)", total_outliers)
                        } else {
                            String::new()
                        }
                    ));
                    debug!("Done with bench {} iter={}", experiment.name, i);

                    i += 1;
                    if i >= experiment.repeat + total_outliers {
                        let outliers = experiment.bench.check_results(&data_path, &dirs).await?;
                        let num_outliers = outliers.len();
                        debug!("num_outliers={num_outliers} size={}", dirs.len());

                        if let Some(max_repeat) = &config.settings.max_repeat {
                            if i >= *max_repeat {
                                debug!("Max repeat reached");
                                debug!("Moving to next test");
                                tokio::fs::write(
                                    results_path.join("info.json"),
                                    serde_json::to_string(&benchmark_info)?,
                                )
                                .await?;
                                break;
                            }
                        }

                        if num_outliers > 0 {
                            for item in &outliers {
                                benchmark_info.remove(&dirs[*item]);
                                debug!("Removing {}", dirs[*item]);
                                remove_dir_all(&data_path.join(&dirs[*item])).await?;
                                tracker.lock().await.increment();
                                pb.inc(1);
                            }
                            remove_indices(&mut dirs, &outliers);
                        }

                        tokio::fs::write(
                            results_path.join("info.json"),
                            serde_json::to_string(&benchmark_info)?,
                        )
                        .await?;
                        if num_outliers == 0 {
                            debug!("Moving to next test");
                            break;
                        } else {
                            warn!("{num_outliers} outliers found");
                            total_outliers += num_outliers;
                        }
                    }
                }
                experiment_dirs.extend(dirs);
            }
        }

        pb.set_message(format!(
            "Experiment {}/{}: {}",
            current_experiment,
            total_experiments,
            style("Generating plots...").dim()
        ));

        plot(
            &experiment.plots,
            PlotType::Individual,
            &data_path,
            &plot_path,
            &config,
            &benchmark_info,
            experiment_dirs.clone(),
            &config.settings,
            &mut Vec::new(),
        )
        .await?;

        debug!("Plotting done");
    }

    pb.finish_with_message(format!(
        "Completed in {} | Avg: {}/iter",
        tracker.lock().await.elapsed(),
        format_seconds(
            tracker.lock().await.start_time.elapsed().as_secs_f64()
                / tracker.lock().await.total_units as f64
        )
    ));

    sleep(Duration::from_secs(3)).await;

    for s in sensors {
        s.send_async(SensorRequest::Quit).await?;
    }

    for s in sensor_handles {
        s.await??;
    }

    debug!("Exiting");
    Ok(())
}

fn get_bench_args(
    bench_args: &[Box<dyn BenchArgs>],
    bench: &dyn Bench,
) -> Result<Box<dyn BenchArgs>> {
    for args in bench_args {
        if args.name() == bench.name() {
            return Ok(args.clone());
        }
    }
    bail!("Could not find bench args for bench {}", bench.name())
}

fn get_sensor_args(
    sensor_args: &[Box<dyn SensorArgs>],
    sensor: &dyn Sensor,
) -> Result<Box<dyn SensorArgs>> {
    for args in sensor_args {
        if args.name() == sensor.name() {
            return Ok(args.clone());
        }
    }
    bail!("Could not find sensor args for sensor {}", sensor.name())
}

fn calculate_total_units(config: &Config) -> u64 {
    config.benches.iter().fold(0, |acc, exp| {
        let power_states = match &config.settings.nvme_power_states {
            Some(ps) => ps.len(),
            None => 1,
        };

        let bench_args = get_bench_args(&config.bench_args, &*exp.bench).unwrap();
        let commands = exp
            .bench
            .cmds(&config.settings, &*bench_args, &exp.name)
            .unwrap()
            .1
            .len();
        let iterations = config.settings.max_repeat.unwrap_or(exp.repeat) as u64;
        acc + power_states as u64 * commands as u64 * iterations
    })
}
struct TimingTracker {
    start_time: Instant,
    total_units: u64,
    completed_units: u64,
}

impl TimingTracker {
    fn new(total: u64) -> Self {
        Self {
            start_time: Instant::now(),
            total_units: total,
            completed_units: 0,
        }
    }

    fn increment(&mut self) {
        self.completed_units += 1;
    }

    fn eta(&self) -> String {
        if self.completed_units == 0 {
            return "N/A".to_string();
        }

        let elapsed = self.start_time.elapsed().as_secs_f64();
        let avg_time_per_unit = elapsed / self.completed_units as f64;
        let remaining_units = self.total_units.saturating_sub(self.completed_units);
        let eta_seconds = avg_time_per_unit * remaining_units as f64;
        format_seconds(eta_seconds)
    }

    fn elapsed(&self) -> String {
        format_seconds(self.start_time.elapsed().as_secs_f64())
    }
}

fn format_seconds(seconds: f64) -> String {
    let hours = (seconds / 3600.0) as u64;
    let minutes = ((seconds % 3600.0) / 60.0) as u64;
    let seconds = (seconds % 60.0) as u64;

    if hours > 0 {
        format!("{:02}:{:02}:{:02}", hours, minutes, seconds)
    } else {
        format!("{:02}:{:02}", minutes, seconds)
    }
}
