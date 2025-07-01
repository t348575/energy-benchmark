# netio-http

This sensor is intended for use with [Netio](https://www.netio-products.com/en) PDU's, and pulls power metrics using their HTTP API (limited to 0.5 reqs/s).

The following are recorded to `netio-http.csv`:
* Voltage
* Current
* Total load
* Output 1 load
* Output 2 load

## Configuration
To use netio-http, add `NetioHttp` to the `sensors` list in your configuration yaml, then add the config citing the url of the PDU to `sensor_args`:
```
- type: NetioHttpConfig
  url: http://some-ip/netio.json
```