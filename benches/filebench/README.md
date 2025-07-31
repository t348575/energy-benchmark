# filebench
Use [filebench](https://github.com/filebench/filebench) to run workloads.

## Prerequisites
1. Install filebench
2. Ensure `mkfs` is available on your system.

## Configuration
Set `FilebenchConfig` if you have a custom path to filebench:
```yaml
bench_args:
  - type: FilebenchConfig
    program: path_to_custom_filebench
```

To use filebench, add `Filebench` as a bench, then specify arguments:
```yaml
benches:
  - name: test
    repeat: 1
    bench:
      type: Filebench
      job_file: fileserver # workload file
      fs: [Ext4, Xfs, Btrfs] # filesystems to test
      runtime: 60 # runtime of workload after fileset has been created
      prefill: 512G # used to "prefill" the SSD
      #               with a large file to prevent SSD from being empty
      #               check below for configuration
      vars: # workload variables to set 
        - meanfilesize: 128k
          nfiles: 250000
          nthreads: 10
        - meanfilesize: 512k
          nfiles: 250000
          nthreads: 10
```

For performing prefill, enable the cargo feature in `setup.toml`:
```yaml
[filebench]
features = ["prefill"]
```