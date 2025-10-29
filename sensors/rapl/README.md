# rapl
A sensor that reads `/sys/class/powercap/` Intel RAPL files to record CPU & DRAM energy.

Total power, followed by package and DRAM power for each CPU like so: `Total,package-0,dram-0` are saved to `rapl.csv`

## Configuration
To use rapl, add `Rapl` to the `sensors` list in your configuration yaml, no configuration required.
```
sensors:
  - sensor: Rapl
```