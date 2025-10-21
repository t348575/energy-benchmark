# h5bench
Run [h5bench](https://github.com/hpc-io/h5bench) benchmarks.

## Prerequisitess
1. Clone and build [hdf5](https://github.com/HDFGroup/hdf5) and [h5bench](https://github.com/hpc-io/h5bench) based on their READMEs.
2. Ensure [OpenMPI](https://www.open-mpi.org/) is installed.
**Note**: It is suggested to install hdf5 and h5bench to a local path.

## Configuration
To use h5bench, add `H5Bench` as a bench, then specify arguments:
```yaml
benches:
  - name: test
    repeat: 1
    bench:
      type: H5Bench
      rank: 10 # number of cores, i.e. MPI rank
      benchmark: write # which h5bench benchmark to run
      base_fs: Ext4 # base filesystem to use on the SSD
      configuration: # configuration parameters for the benchmark
        MEM_PATTERN: CONTIG
        FILE_PATTERN: CONTIG
        TIMESTEPS: 100
        NUM_DIMS: 2
        DIM_1: 4096
        DIM_2: 4096
        MODE: SYNC
```
