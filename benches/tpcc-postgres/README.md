# tpcc-postgres
Run [TPC-C](https://www.tpc.org/tpcc/default5.asp) on [PostgreSQL](https://www.postgresql.org/) using [tpcc-postgres](https://github.com/ydb-platform/tpcc-postgres).

## Prerequisites
1. Install [Docker](https://www.docker.com/)
2. Clone the [tpcc-postgres](https://github.com/ydb-platform/tpcc-postgres) repository.
3. Run [build-images.sh](build-images.sh) to build the client and host docker images.
4. Clone the [benchhelpers](https://github.com/ydb-platform/benchhelpers/) repository.

## Configuration
Set `TpccPostgresConfig` with the path to the tpcc repository and to the benchhelpers repository:
```yaml
bench_args:
  - type: TpccPostgresConfig
    benchhelpers: path_to_benchhelpers
    tpcc_postgres: path_to_tpcc-postgres
```

To use tpcc-postgres, add `TpccPostgres` as a bench, then specify arguments:
```yaml
benches:
  - name: test
    repeat: 1
    bench:
      type: TpccPostgres
      num_clients: [1]
      warehouses: 2000
      config_file: ./my_custom_postgres_config # optional
      filesystem: Ext4
      fs_mount_opts: defaults,commit=60,data=ordered # optional, mounting options for filesystem
```