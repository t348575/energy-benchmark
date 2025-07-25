use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    process::Stdio,
    sync::Arc,
    time::{Duration, Instant},
};

use chrono::Local;
use common::{
    bench::{Bench, BenchArgs, BenchmarkInfo, Cmd, CmdsResult},
    config::Config,
    plot::{PlotType, plot},
    sensor::{Sensor, SensorArgs, SensorRequest},
    util::{chown_user, remove_indices, simple_command_with_output_no_dir},
};
use console::style;
use eyre::{Context, Result, bail};
use flume::unbounded;
use indicatif::{ProgressBar, ProgressStyle};
use tokio::{
    fs::{copy, create_dir_all, read_to_string, remove_dir_all, write},
    process::Command,
    spawn,
    sync::Mutex,
    time::sleep,
};
use tracing::{debug, error, info, warn};

pub async fn run_benchmark(config_file: String, no_progress: bool, skip_plot: bool) -> Result<()> {
    let config: Config = serde_yml::from_str(&read_to_string(&config_file).await?)?;
    let unique_bench_names = config
        .benches
        .iter()
        .map(|x| &x.name)
        .collect::<HashSet<_>>();
    if unique_bench_names.len() != config.benches.len() {
        bail!(
            "Bench names must be unique! Config file contains multiple benchmarks with the same name."
        );
    }

    _ = simple_command_with_output_no_dir("umount", &[&config.settings.device]).await;
    let file_prefix = Local::now().format("%Y-%m-%d_%H-%M-%S").to_string();
    println!(
        "Results created in folder: results/{}-{file_prefix}",
        config.name
    );

    let progress = Progress::new(!no_progress, &config)?;

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
    let results_path = PathBuf::from("results").join(format!("{}-{file_prefix}", config.name));
    let data_path = results_path.join("data");
    create_dir_all(&results_path).await?;
    let plot_path = results_path.join("plots");
    _ = remove_dir_all(&plot_path).await;
    create_dir_all(&plot_path).await?;
    copy(config_file, results_path.join("config.yaml")).await?;

    debug!("Initial results setup done!");
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
    let mut append_spdk_power_state = false;

    for experiment in &config.benches {
        current_experiment += 1;
        let mut ps = nvme_power_states.clone();
        if ps.is_empty() {
            ps.push(-1);
        }

        let mut last_experiment: Option<Box<dyn Bench>> = None;
        let mut experiment_dirs = Vec::new();
        let bench_args = get_bench_args(&config.bench_args, &*experiment.bench);
        let CmdsResult { cmds, program } =
            experiment
                .bench
                .cmds(&config.settings, &*bench_args, &experiment.name)?;

        let total_commands = cmds.len();
        for power_state in &ps {
            for (
                curr_cmd_idx,
                Cmd {
                    args,
                    hash,
                    bench_obj,
                },
            ) in cmds.iter().enumerate()
            {
                if *power_state != -1 {
                    if bench_obj.requires_custom_power_state_setter() {
                        if bench_obj.name() == "fio" {
                            append_spdk_power_state = true;
                        }
                    } else {
                        append_spdk_power_state = false;
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
                }

                let mut i = 0;
                let mut total_outliers = 0;
                let mut dirs = Vec::new();
                loop {
                    progress.set_message(format!(
                        "Experiment {}/{} | PS {} | Cmd {}/{}: Iteration {}{}",
                        current_experiment,
                        total_experiments,
                        if *power_state == -1 {
                            "N/A".to_owned()
                        } else {
                            power_state.to_string()
                        },
                        curr_cmd_idx + 1,
                        total_commands,
                        i,
                        if total_outliers > 0 {
                            format!(" ({total_outliers} retries)")
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
                            args: bench_obj.clone(),
                            power_state: *power_state,
                            iteration: i,
                            name: experiment.name.clone(),
                            hash: hash.clone(),
                        },
                    );
                    create_dir_all(&final_path).await?;
                    chown_user(&final_path).await?;

                    bench_obj
                        .experiment_init(
                            &data_path,
                            &config.settings,
                            &*bench_args,
                            &last_experiment,
                        )
                        .await?;
                    chown_user(&final_path).await?;

                    let mut args = args.clone();
                    bench_obj.add_path_args(&mut args, &final_path);

                    if bench_obj.name() == "fio" && append_spdk_power_state {
                        args.push(format!("--power_state={power_state}"));
                    }

                    let env = bench_obj
                        .add_env(&*bench_args)
                        .context("Get benchmark env")?;

                    debug!("iter={} program={} args={}", i, program, args.join(" "));
                    let result = bench_obj
                        .run(
                            &program,
                            &args,
                            &env,
                            &config.settings,
                            &sensors,
                            &final_path,
                            bench_obj.clone(),
                            &config,
                            &last_experiment,
                        )
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
                    progress.tick().await;

                    progress.set_message(format!(
                        "Iteration {} [{} retries]",
                        i + 1,
                        if total_outliers > 0 {
                            format!(" ({total_outliers} retries)")
                        } else {
                            "0".to_owned()
                        }
                    ));
                    debug!("Done with bench {} iter={}", experiment.name, i);

                    bench_obj
                        .post_experiment(&data_path, &final_path, &config.settings, &*bench_args)
                        .await
                        .context("Error running post experiment")?;

                    i += 1;
                    if i >= experiment.repeat + total_outliers {
                        let outliers = bench_obj.check_results(&data_path, &dirs).await?;
                        let num_outliers = outliers.len();
                        debug!("num_outliers={num_outliers} size={}", dirs.len());

                        if let Some(max_repeat) = &config.settings.max_repeat
                            && i >= *max_repeat
                        {
                            debug!("Max repeat reached");
                            debug!("Moving to next test");
                            write(
                                results_path.join("info.json"),
                                serde_json::to_string(&benchmark_info)?,
                            )
                            .await?;
                            break;
                        }

                        if num_outliers > 0 {
                            for item in &outliers {
                                benchmark_info.remove(&dirs[*item]);
                                debug!("Removing {}", dirs[*item]);
                                remove_dir_all(&data_path.join(&dirs[*item])).await?;
                                progress.increment_total().await;
                            }
                            remove_indices(&mut dirs, &outliers);
                        }

                        write(
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
                last_experiment = Some(bench_obj.clone());
                experiment_dirs.extend(dirs);
            }
        }
        progress.set_message(format!(
            "Experiment {}/{}: {}",
            current_experiment,
            total_experiments,
            style("Generating plots...").dim()
        ));

        if !skip_plot {
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
        }

        debug!("Plotting done");
        sleep(Duration::from_secs(1)).await;
    }

    progress.finish().await;
    sleep(Duration::from_secs(3)).await;

    for s in sensors {
        s.send_async(SensorRequest::Quit).await?;
    }

    for s in sensor_handles {
        s.await??;
    }

    if !skip_plot {
        let mut completed_dirs = Vec::new();
        for experiment in &config.benches {
            common::plot::plot(
                &experiment.plots,
                PlotType::Total,
                &data_path,
                &plot_path,
                &config,
                &benchmark_info,
                benchmark_info.keys().cloned().collect(),
                &config.settings,
                &mut completed_dirs,
            )
            .await?;
        }
    }

    debug!("Exiting");
    Ok(())
}

fn get_bench_args(bench_args: &[Box<dyn BenchArgs>], bench: &dyn Bench) -> Box<dyn BenchArgs> {
    for args in bench_args {
        if args.name() == bench.name() {
            return args.clone();
        }
    }
    info!("Could not find bench args for bench {}", bench.name());
    bench.default_bench_args()
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

fn calculate_total_units(config: &Config) -> usize {
    config.benches.iter().fold(0, |acc, exp| {
        let power_states = match &config.settings.nvme_power_states {
            Some(ps) => ps.len(),
            None => 1,
        };

        let bench_args = get_bench_args(&config.bench_args, &*exp.bench);
        let commands = exp
            .bench
            .cmds(&config.settings, &*bench_args, &exp.name)
            .unwrap()
            .cmds
            .len();
        acc + power_states * commands * exp.repeat
    })
}
struct TimingTracker {
    start_time: Instant,
    last_completion_time: Instant,
    total_units: u64,
    completed_units: u64,
    initial_estimate: u64,
}

impl TimingTracker {
    fn new(total: u64, initial_estimate: u64) -> Self {
        Self {
            start_time: Instant::now(),
            last_completion_time: Instant::now(),
            total_units: total,
            completed_units: 0,
            initial_estimate,
        }
    }

    fn increment(&mut self) {
        self.completed_units += 1;
        self.last_completion_time = Instant::now();
    }

    fn increment_total(&mut self) {
        self.total_units += 1;
    }

    fn eta(&self) -> String {
        if self.completed_units == 0 {
            return format_seconds(self.initial_estimate as f64 * self.total_units as f64);
        }

        let elapsed_since_last = self.last_completion_time.elapsed().as_secs_f64();
        let elapsed = self.start_time.elapsed().as_secs_f64() - elapsed_since_last;
        let avg_time_per_unit = elapsed / self.completed_units as f64;
        let remaining_units = self.total_units.saturating_sub(self.completed_units);
        let eta_seconds = (avg_time_per_unit * remaining_units as f64) - elapsed_since_last;
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
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes:02}:{seconds:02}")
    }
}

struct Progress(Option<InnerProgress>);

struct InnerProgress {
    pb: ProgressBar,
    tracker: Arc<Mutex<TimingTracker>>,
}

impl Progress {
    fn new(enabled: bool, config: &Config) -> Result<Self> {
        if !enabled {
            return Ok(Progress(None));
        }

        let total_units = calculate_total_units(config);
        let pb = ProgressBar::new(total_units as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{spinner} {wide_bar} {pos}/{len} ({msg})")?
                .tick_chars("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"),
        );
        pb.enable_steady_tick(Duration::from_millis(50));

        let inital_estimate = config.benches.first().unwrap().bench.runtime_estimate()? / 1000;
        debug!(
            "Initial estimate: {}, units: {}",
            inital_estimate, total_units
        );
        let tracker = Arc::new(Mutex::new(TimingTracker::new(
            total_units as u64,
            inital_estimate,
        )));
        let pb_c = pb.clone();
        let t = tracker.clone();
        spawn(async move {
            while !pb_c.is_finished() {
                pb_c.set_message(t.lock().await.eta());
                sleep(Duration::from_millis(250)).await;
            }
        });
        Ok(Progress(Some(InnerProgress { pb, tracker })))
    }

    fn set_message(&self, msg: String) {
        if let Some(s) = &self.0 {
            s.pb.set_message(msg);
        }
    }

    async fn tick(&self) {
        if let Some(s) = &self.0 {
            let mut tracker = s.tracker.lock().await;
            tracker.increment();
            s.pb.inc(1);
            s.pb.set_message(tracker.eta());
        }
    }

    async fn increment_total(&self) {
        if let Some(s) = &self.0 {
            s.tracker.lock().await.increment_total();
        }
    }

    async fn finish(&self) {
        if let Some(s) = &self.0 {
            let tracker = s.tracker.lock().await;
            s.pb.finish_with_message(format!(
                "Completed in {} | Avg: {}/iter",
                tracker.elapsed(),
                format_seconds(
                    tracker.start_time.elapsed().as_secs_f64() / tracker.total_units as f64
                )
            ));

            s.pb.finish();
            s.pb.finish();
        }
    }
}
