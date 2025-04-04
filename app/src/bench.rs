use std::{collections::HashMap, path::PathBuf, time::Duration};

use chrono::Local;
use common::{
    bench::{Bench, BenchArgs, BenchmarkInfo, Cmd},
    config::Config,
    sensor::{Sensor, SensorArgs, SensorRequest},
    util::remove_indices,
};
use eyre::{Context, Result, bail};
use flume::unbounded;
use tokio::{
    fs::{copy, create_dir_all, read_to_string, remove_dir_all},
    process::Command,
    time::sleep,
};
use tracing::{debug, error, warn};

pub async fn run_benchmark(config_file: String) -> Result<()> {
    let config: Config = serde_yml::from_str(&read_to_string(&config_file).await?)?;

    let sensor_objects = default_sensor::SENSORS.get().unwrap().lock().unwrap();
    let mut sensors = Vec::new();
    let mut sensor_replies = Vec::new();
    let mut loaded_sensors = Vec::new();
    let mut sensor_handles = Vec::new();

    for s in config.sensors {
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

    debug!("Loaded sensors: {loaded_sensors:?}");

    let file_prefix = Local::now().format("%Y-%m-%d_%H-%M-%S").to_string();
    println!(
        "Results created in folder: results/{}-{file_prefix}",
        config.name
    );
    let results_path = PathBuf::from("results").join(format!("{}-{file_prefix}", config.name));
    let data_path = results_path.join("data");
    create_dir_all(&results_path).await?;
    copy(config_file, results_path.join("config.yaml")).await?;

    let nvme_power_states = match &config.settings.nvme_power_states {
        Some(ps) => ps.into_iter().map(|x| *x as i32).collect(),
        None => vec![],
    };

    let nvme_cli_device = config
        .settings
        .nvme_cli_device
        .clone()
        .unwrap_or(config.settings.device.clone());

    let mut benchmark_info = HashMap::new();
    for experiment in config.benches {
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
                    .spawn()
                    .context("Set nvme power state")?;
                let status = ps_change_cmd.wait().await?;

                if !status.success() {
                    bail!("Could not change device power state to {power_state}");
                }
                debug!("Power state of {nvme_cli_device} change to {power_state}");
            }

            let bench_args = get_bench_args(&config.bench_args, &*experiment.bench)?;
            let (program, cmds) =
                experiment
                    .bench
                    .cmds(&config.settings, &*bench_args, &experiment.name)?;

            for Cmd {
                args,
                hash,
                arg_obj,
            } in cmds
            {
                let mut i = 0;
                let mut total_outliers = 0;
                let mut dirs = Vec::new();
                loop {
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
                            power_state: power_state,
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
        experiment
            .bench
            .plot(
                &data_path,
                &results_path,
                &benchmark_info,
                experiment_dirs.clone(),
                &config.settings,
            )
            .await?;
    }

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
