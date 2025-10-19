# diskstat
The diskstat sensor is a basic sensor that reads `/sys/block/<device>/stat`, and represents the actual I/O sent to the device.

The following is recorded in `diskstat.csv`:
* Reads
* Writes
* Read bandwidth (bytes/s)
* Write bandwidth (bytes/s)
* Read I/O's
* Write I/O's
* Read merges
* Write merges
* Time in queue
* Read ticks
* Write ticks

## Configuration
To use diskstat, add `DiskStat` to the `sensors` list in your configuration yaml, then add the config citing the disk to `sensor_args` (nvme2n1 shown as an example):
```
- type: DiskStatConfig
```