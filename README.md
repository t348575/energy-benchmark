# nvme-energy-bench ![Visits](https://lambda.348575.xyz/repo-view-counter?repo=nvme-energy-bench)
A tool to automate NVMe SSD energy-performance benchmarks.

## Setup
1. Clone this repository.
2. Configure [setup.toml](setup.toml) with the benchmarks, sensors and plotters you require (just the name)
#### Example:
```toml
benches = ["fio", "ycsb"]
sensors = ["powersensor3", "sysinfo"]
plots = ["ycsb-basic"]

[ycsb]
features = ["prefill"] # activate a cargo feature for this benchmark
```
3. Ensure all dependencies required for the benchmark runners, sensors & plotters are installed (check respective directories for README)
4. Run `cargo build` (populates dependencies from setup.toml)
5. Run `cargo build --release -p nvme-energy-bench` (built executable in `target/release/`)
6. Ensure `python3` is installed if you are generating any plots, preferably create a virtual env as well.
7. Setup a `config.yaml` benchmark configuration file as shown below, then run using: `sudo target/release/nvme-energy-bench bench`

**Note 1**: Always run the benchmark using sudo, and from the repository root.

**Note 2**: Set the `RUST_LOG` environment variable to emit logs (debug, info, warn, error)

## Benchmark config
For specific configuration options for each benchmark, sensor or plotter, check respective README.

Example configuration:
```yaml
name: rocksdb                                   # Prefix for result folder
settings:
  device: /dev/nvme2n1                          # Device to run benchmarks on
  numa:                                         # Optional, force a NUMA configuration, will pass the option to the benchmark if it supports, else uses numactl
    cpunodebind: 1
    membind: 1
  nvme_power_states: [0, 1]                     # Optional, NVMe power states to test, will not set any state by default.
  max_repeat: 5                                 # Optional, Maximum number of repetitions of each benchmark configuration, will not perform repetitions if not set.
  should_trace: true                            # Optional, Use bpftrace to trace NVMe calls, disabled by default.
  cpu_max_power_watts: 200                      # Your CPU's maximum rated power, used for filtering faulty readings during plot generation
  sleep_between_experiments: 60                 # Optional, benchmark sits idle for X seconds after each experiment
  sleep_after_writes: 60                        # Optional, benchmark sits idle for X seconds after each experiment only IF write_hint returns true
                                                # i.e. if the experiment might have performed write operations (to allow for GC settle)
  cpu_freq:                                     # Optional, Limit CPU frequency.
    freq: 1200000
    default_governor: schedutil                 # Default frequency governor to return to after the benchmark
  cgroup:                                       # Optional, Use Cgroup v2 IO limits.
    cpuset:                                     # Optional
      cpus: [[10, 20]]                          # Optional specify cgroup cpu range
      mems: [[1, null]]                         # Optional, specify memory numa range
    io:                                         # Optional
      max:                                      # Optional, io.max.
        bps:                                    # specify bps or iops
          r: 629145600
          w: 629145600
      weight: 200                               # Optional, io.weight.
      latency: 50                               # Optional, io.latency.
      cost:
        qos: Auto                               # Optional, io.cost.qos, specify Auto or User.
        # qos: !User
        #   pct:
        #     r: 45
        #     w: 65
        #   latency:
        #     r: 10
        #     w: 30
        #   scaling:
        #     min: 10
        #     max: 85
        model: Auto                             # Optional, io.cost.model, specify Auto or User.
        # model: !User
        #   bps:
        #     r: 629145600
        #     w: 104857600
        #   seqiops:
        #     r: 1000
        #     w: 5000
        #   randiops:
        #     r: 10000
        #     w: 10000


bench_args:                                     # Global arguments for benchmarks, always suffixed with `Config` consult specific benchmark README
  - type: YcsbConfig
    root_dir: ./ycsb-0.17.0
  - type: FioConfig
    program: ../fio/fio

sensors:                                        # Sensors to record
  - sensor: Powersensor3
    args:
      type: Powersensor3Config
      device: /dev/ttyACM0
  - sensor: Rapl
  - sensor: Sysinfo
    args:                                       # Specify sensor arguments if required, always suffixed with `Config` consult specific sensor README.
      type: SysinfoConfig
      interval: 10

benches:                                        # Benchmarks
  - name: a                                     # Name to prefix result data directory
    repeat: 1                                   # Minimum repetitions
    bench:                                      # Benchmark specific arguments, consult specific benchmark README
      type: Ycsb
      workload_file: workloads/workloada
      fs: Ext4
      data_var_name: rocksdb.dir
      db: rocksdb
      trace: true
      threads: 16
      prefill: 256G
      vars:
        operationcount: 10000000
        recordcount: 10000000
    plots:                                      # Plotter specific arguments, consult specific plotter README
      - type: YcsbBasic
      - type: YcsbPowerTime
```

## Adding new sensors

Adding new sensors involves implementing the `Sensor` trait in [common/src/sensor.rs](common/src/sensor.rs):
```rs
/// All [`Sensor`] implementations are expected to implement [`Default`]
pub trait Sensor: Debug + Send + Sync {
    /// Name of the sensor, for identification
    fn name(&self) -> sensor_common::SensorKind;
    /// Sensor data filename
    fn filename(&self) -> &'static str;
    /// Should start an async task that collects sensor data using [`tokio::task::spawn`]
    ///
    /// Arguments:
    /// * `args` - Specific arguments to the sensor
    /// * `settings` - Global settings from the config file
    /// * `rx` - Requests to the sensor to start/stop recording
    /// * `tx` - Replies from the sensor when its done flushing data to disk, after [`SensorRequest::StopRecording`] is received
    fn start(
        &self,
        args: &dyn SensorArgs,
        settings: &Settings,
        rx: Receiver<SensorRequest>,
        tx: Sender<SensorReply>,
    ) -> Result<JoinHandle<Result<()>>>;
}
```

The following shows the minimal implementation for an example sensor `DummySensor`. Consult the other implemented sensors to get a better idea on implmentation.
```rs
/// This represents sensor configuration from the YAML configuration provided under "sensors:"
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DummySensorConfig {
  some_value: u32
}

/// This is for internal matching & serde parsing
#[typetag::serde]
impl SensorArgs for DummySensorConfig {
    fn name(&self) -> SensorKind {
        SensorKind::DummySensor
    }
}

/// The sensor. All derives shown here are compulsory.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DummySensor;

struct InternalDummySensor {
    some_internal_data: Vec<u32>
}

const SENSOR_FILENAME: &str = "dummy_sensor.csv";

impl Sensor for DummySensor {
  fn name(&self) -> SensorKind {
      SensorKind::DummySensor
  }

  /// Filename for the created csv/json (or whatever).
  fn filename(&self) -> &'static str {
    SENSOR_FILENAME
  }

  /// This spawns a tokio async function used to poll the sensor data. If you need a dedicated thread, spawn a blocking tokio task inside a tokio async function, as done in sensors/powersensor3/src/lib.rs
  fn start(
    &self,
    args: &dyn SensorArgs,
    settings: &Settings,
    rx: Receiver<SensorRequest>,
    tx: Sender<SensorReply>,
  ) -> Result<JoinHandle<Result<()>>> {
    let args = args
      .downcast_ref::<RaplConfig>()
      .context("Invalid sensor args, expected args for Rapl")?;

    // Required for Send/Sync.
    let args = args.clone();
    let handle = spawn(async move {
      // A default sensor poller function provided in common/src/util.rs
      // sensor_reader automatically manages sensor startup, shutdown, errors, accuratly polling the sensors, and flushing results to disk. It is recommended to directly utilize this utility function.
      if let Err(err) = sensor_reader(
        rx, // to handle sensor recording start/stop, shutdown.
        tx, // to handle sensor recording start/stop, shutdown.
        SENSOR_FILENAME,
        args,
        init_dummysensor, // a function to perform one time initialization for the sensor. Also returns the output columns.
        |args, sensor, sensor_request, last_time|
          -> std::pin::Pin<
            Box<dyn Future<Output = Result<Vec<f64>, SensorError>> + Send>,
        > { Box::pin(read_dummysensor(sensor.clone(), last_time)) },
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

async fn init_dummysensor(
  config: DummySensorConfig,
) -> Result<(Arc<Mutex<InternalDummySensor>>, Vec<String>)> {
  // Do any initialization work
  Ok((
    Arc::new(Mutex::new(InternalDummySensor {
      some_internal_data: Vec::new()
    })),
    // The columns names for each value in the collected data.
    vec![
        "col_1",
        "col_2",
    ]
    .into_iter()
    .map(|x| x.to_owned())
    .collect(),
  ))
}

async fn read_dummysensor(sensor: Arc<Mutex<InternalDiskStat>>, last_time: Instant) -> Result<Vec<f64>, SensorError> {
    let mut sensor = sensor.lock().await;
    // Read the sensor, collect data, etc.
    let mut readings = read_sensor_data(); // Your sensor collection logic
    // Your sensor interval. Use async_io sleep rather than tokio sleep, since it is far more accurate than tokio sleep for small sleep intervals like 1ms.
    async_io::Timer::after(Duration::from_micros(1000)).await;
    // The recorded sensor data
    Ok(readings)
}
```

## Adding new benchmarks

Adding new benchmarks involves implementing the `Bench` trait in [common/src/bench.rs](common/src/bench.rs):
Documentation provided in `bench.rs` sufficienctly details the methods. The provided implementations in this repostiory can be used as examples, such as [fio](benches/fio) and [filebench](benches/filebench/).

The main benchmark flow is as follows:
1. `cmds()` is called for each yaml benchmark, and should return every benchmark variant to be executed, along with its CLI arguments
2. For each power state configured:
   1. NVMe device power state for the run is set
   2. For each benchmark variant returned by `cmds()`
      1. The data subsdirectory where results are stored for the run is created
      2. `experiment_init()`
      3. `add_path_args()` allow mutating the CLI arguments for this run
      4. `add_env()` to set additional enviroment variables
      5. `run()` the actual benchmark run
      6. Wait for sensors to finish writing files.
      7. `post_experiment()` is called to perform any required cleanup
      8. Benchmark sleeps if required based on write hint, useful to allow any SSD GC to settle