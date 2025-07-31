# ycsb
Use [ycsb](https://github.com/brianfrankcooper/YCSB) to run workloads.

## Prerequisites
1. Download and extract the latest [ycsb](https://github.com/brianfrankcooper/YCSB) release.

## Configuration
Set `YcsbConfig` with the path to your ycsb installation.
```yaml
bench_args:
  - type: YcsbConfig
    root_dir: path_to_ycsb
```

To use ycsb, add `Ycsb` as a bench, then specify arguments:
```yaml
benches:
  - name: test
    repeat: 1
    bench:
      type: Ycsb
      workload_file: workloads/workloada # base_path is your ycsb_root_dir
      data_var_name: rocksdb.dir # variable used to set data directory
      db: rocksdb # database to test
      fs: Ext4
      threads: 16 # optional
      prefill: 200GB # optional prefill file
      fs_mount_opts: # optional filesystem mount options
      vars: # optional extra options
        operationcount: 100000000
        recordcount: 100000000
```

For performing prefill, enable the cargo feature in `setup.toml`:
```yaml
[ycsb]
features = ["prefill"]
```