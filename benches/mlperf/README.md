# mlperf
Run [MLPerf Storage](https://github.com/mlcommons/storage) workloads.

## Prerequisitess
1. Clone the repository [MLPerf Storage](https://github.com/mlcommons/storage).
2. Follow their README for dependenicies to build & install MLPerf Storage.
**Note**: If MLPerf storage is installed in a virtual environment, ensure the env is at the root of this repository.

## Configuration
To use mlperf, add `Mlperf` as a bench, then specify arguments:
```yaml
benches:
  - name: test
    repeat: 1
    bench:
      type: Mlperf
      model: Unet3d # Unet3d, Resnet50, Cosmoflow
      memory_gb: 16 # Host memory to use
      n_accelerators: [2, 4] # How many accelerators/gpus to use
      accelerator_type: A100 # A100, H100
      params: # extra parameters to pass
        dataset.num_files_train: 4000
```
