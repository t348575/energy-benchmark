# sysinfo
Reads various files updated by the linux kernel to fetch CPU information.

The following is recorded in `sysinfo.csv`:
* Frequencies of each CPU core
* Linux load on each CPU core
* System memory used
* CPU (load) used by benchmark application
* Memory used by benchmark applicatoin

## Configuration
To use sysinfo, add `Sysinfo` to the `sensors` list in your configuration yaml, then add the config to `sensor_args`.

**NOTE**: Sysinfo data collection can be CPU intensive, so intervals of less than 10ms is not recommended.
```
- type: SysinfoConfig
  interval: 10 # data collection frequency in milliseconds
```