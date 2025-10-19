# powersensor_3
[PowerSensor3](https://github.com/nlesc-recruit/PowerSensor3) is a custom hardware tool to measure power consumption of PCIe devices.

## Prerequisites
1. Ensure a C++ compiler is available
2. Install the [PowerSensor3](https://github.com/nlesc-recruit/PowerSensor3) library

The following fields are recorded to `powersensor3.csv`:
* Total
* PCIe_3V3 (power on 3.3v PCIe)
* PCIe_12V (power on 12v PCIe)

## Configuration
To use Powersensor3, add `Powersensor3` to the `sensors` list in your configuration yaml, then add the config to `sensor_args`:

`/dev/ttyACM0` is an example location for the serial interface to the sensor.
```
- type: Powersensor3Config
  device: /dev/ttyACM0
```