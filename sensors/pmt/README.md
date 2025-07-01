# pmt

[PMT](https://git.astron.nl/RD/pmt) is a power measurment toolkit integrated with various power measurement systems. It is not recomended to use it due reporting inaccuracies. Only PMT's RAPL interface is supported as of now.

The data recorded to `rapl.csv` depends on the sensor indexes specified.

## Configuration
To use pmt, add `PmtConfig` to the `sensors` list in your configuration yaml, then add the config citing the sensor indexes and sensor type to `sensor_args`:
```
- type: PmtConfig
  indexes: [-1, 0, 1, 2, 3, 4]
  sensor: RAPL
```