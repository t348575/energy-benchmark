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
6. Setup a `config.yaml` benchmark configuration file as shown below, then run using: `sudo target/release/energy-benchmark bench`
**Note 1**: Always run the benchmark using sudo, and from the repository root.
**Note 2**: Set the `RUST_LOG` environment variable to emit logs (debug, info, warn, error)

## Benchmark config
For specific configuration options for each benchmark, sensor or plotter, check respective README.

Example configuration:
```yaml
name: rocksdb                                   # prefix for result folder
settings:
  device: /dev/nvme2n1                          # device to run benchmarks on
  numa:                                         # force a numa configuration. Optional, will pass the option to the benchmark if it supports, else uses numactl
    cpunodebind: 1
    membind: 1
  nvme_power_states: [0, 1]                     # nvme power states to test
  nvme_cli_device: /dev/nvme2                   # the device root
  max_repeat: 5                                 # maximum number of repetitions of each benchmark configuration

bench_args:                                     # global arguments for benchmarks, always suffixed with `Config` consult specific benchmark README
  - type: YcsbConfig
    root_dir: ./ycsb-0.17.0
  - type: FioConfig
    program: ../fio/fio

sensors: [Powersensor3, Sysinfo]                # sensors to record
sensor_args:                                    # sensor arguments always compulsory, always suffixed with `Config` consult specific sensor README
  - type: Powersensor3Config
    device: /dev/ttyACM0
    indexes: [-1, 1, 2]
  - type: SysinfoConfig
    interval: 10

benches:                                        # benchmarks
  - name: a                                     # name to prefix result data directory
    repeat: 1                                   # minimum repetitions
    bench:                                      # benchmark specific arguments, consult specific benchmark README
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
    plots:                                      # plotter specific arguments, consult specific plotter README
      - type: YcsbBasic
      - type: YcsbPowerTime
```