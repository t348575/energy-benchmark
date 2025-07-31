# fio
Use [fio](https://github.com/axboe/fio) to run workloads.

## Prerequisites
1. Install fio

## SPDK Prerequisites
1. Clone [spdk](https://github.com/spdk/spdk) and setup the repository based on their README.
2. Copy [fio_plugin.c](fio_plugin.c) to the SPDK repository replacing the existing file at `app/fio/nvme/fio_plugin.c`.
3. Build SPDK.

## Configuration
Set `FioConfig` if you have a custom path and other optional args. SPDK path is compulsory if using SPDK:
```yaml
bench_args:
  - type: FioConfig
    program: path_to_custom_fio
    log_avg: 10 # Default is 10 if not specified
    spdk_path: path_to_spdk_repository
```
* `log_avg` corresponds to [log_avg_msec]([](https://fio.readthedocs.io/en/latest/fio_doc.html#cmdoption-arg-log_avg_msec)) fio argument.

To use fio, add `Fio` as a bench, then specify arguments:
```yaml
benches:
  - name: test
    repeat: 1
    bench:
      type: Fio
      test_type:
        type: read # read, write, read_write, rand_read, rand_write, rand_read_write
        args: # only specify for read_write or rand_read_write
          read: 30
          write: 70
      request_sizes: [4k]
      io_engines: [libaio]
      io_depths: [32]
      direct: true
      time_based: true
      runtime: 60s # optional
      ramp_time: 10s # optional
      size: 10G # optional
      num_jobs: [2] # optional
      extra_options: [[--thread], [--thinktime=8ms, --thinktime_blocks=4]] # optional
```
