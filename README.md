# energy-benchmark ![Visits](https://lambda.348575.xyz/repo-view-counter?repo=energy-benchmark)
A tool to automate NVME SSD energy-performance benchmarks

## Setup
1. Clone the repository https://github.com/t348575/energy-benchmark
2. Configure [setup.toml](setup.toml) with the benchmarks, sensors and plotters you require (just the name)
#### Example:
```toml
benches = ["fio", "ycsb"]
sensors = ["powersensor_3", "sysinfo"]
plots = ["ycsb-basic"]

[ycsb]
features = ["prefill"] # activate a cargo feature for this benchmark
```
3. Ensure all dependencies required for the benchmark runners, sensors & plotters are installed (check respective directories for README)
4. Run `cargo build` (populates dependencies from setup.toml)
5. Run `cargo build --release -p energy-benchmark` (built executable in `target/release/`)
6. Ensure `python3` is installed if you are generating any plots, preferibly create a virtual env as well.
7. Setup a `config.yaml` benchmark configuration file as shown below, then run using: `sudo target/release/energy-benchmark bench`

**Note 1**: Always run the benchmark using sudo, and from the repository root.

**Note 2**: Set the `RUST_LOG` environment variable to emit logs (debug, info, warn, error)

## Benchmark config
For specific configuration options for each benchmark, sensor or plotter, check respective README.

Example configuration:
```yaml
name: rocksdb                                   # Prefix for result folder
settings:
  device: /dev/nvme2n1                          # Device to run benchmarks on
  numa:                                         # Force a NUMA configuration. Optional, will pass the option to the benchmark if it supports, else uses numactl
    cpunodebind: 1
    membind: 1
  nvme_power_states: [0, 1]                     # NVMe power states to test. Optional, will not set any state by default
  max_repeat: 5                                 # Maximum number of repetitions of each benchmark configuration. Optional, will not perform repetitions if not set 
  should_trace: true                            # Use bpftrace to trace NVMe calls. Optional, disabled by default
  cpu_max_power_watts: 200                      # Your CPU's maximum rated power, used for filtering faulty readings during plot generation
  cpu_freq:                                     # Limit CPU frequency, Optional.
    freq: 1200000
    default_governor: schedutil                 # Default frequency governor to return to after the benchmark
  cgroup_io:                                    # Use Cgroup v2 IO, Optional.
    max:                                        # io.max. Optional.
      bps:                                      # specify bps or iops
        r: 629145600
        w: 629145600
    weight: 200                                 # io.weight. Optional.
    latency: 50                                 # io.latency. Optional.
    cost:
      qos: Auto                                 # io.cost.qos. Optional, specify Auto or User.
      # qos: !User
      #   pct:
      #     r: 45
      #     w: 65
      #   latency:
      #     r: 10
      #     w: 30
      model: Auto                               # io.cost.model. Optional, specify Auto or User.
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

sensors: [Powersensor3, Sysinfo]                # Sensors to record
sensor_args:                                    # Sensor arguments always compulsory, always suffixed with `Config` consult specific sensor README
  - type: Powersensor3Config
    device: /dev/ttyACM0
  - type: SysinfoConfig
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