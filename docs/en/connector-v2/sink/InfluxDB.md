# InfluxDB

> InfluxDB sink connector

## Description

Write data to InfluxDB.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

## Options

| name                        | type   | required | default value                |
|-----------------------------|--------|----------|------------------------------|
| url                         | string | yes      | -                            |
| database                    | string | yes      |                              |
| measurement                 | string | yes      |                              |
| username                    | string | no       | -                            |
| password                    | string | no       | -                            |
| key_time                    | string | no       | processing time              |
| key_tags                    | array  | no       | exclude `field` & `key_time` |
| batch_size                  | int    | no       | 1024                         |
| batch_interval_ms           | int    | no       | -                            |
| max_retries                 | int    | no       | -                            |
| retry_backoff_multiplier_ms | int    | no       | -                            |
| connect_timeout_ms          | long   | no       | 15000                        |
| common-options              | config | no       | -                            |

### url
the url to connect to influxDB e.g.
``` 
http://influxdb-host:8086
```

### database [string]

The name of `influxDB` database

### measurement [string]

The name of `influxDB` measurement

### username [string]

`influxDB` user username

### password [string]

`influxDB` user password

### key_time [string]

Specify field-name of the `influxDB` measurement timestamp in SeaTunnelRow. If not specified, use processing-time as timestamp

### key_tags [array]

Specify field-name of the `influxDB` measurement tags in SeaTunnelRow.
If not specified, include all fields with `influxDB` measurement field

### batch_size [int]

For batch writing, when the number of buffers reaches the number of `batch_size` or the time reaches `batch_interval_ms`, the data will be flushed into the influxDB

### batch_interval_ms [int]

For batch writing, when the number of buffers reaches the number of `batch_size` or the time reaches `batch_interval_ms`, the data will be flushed into the influxDB

### max_retries [int]

The number of retries to flush failed

### retry_backoff_multiplier_ms [int]

Using as a multiplier for generating the next delay for backoff

### max_retry_backoff_ms [int]

The amount of time to wait before attempting to retry a request to `influxDB`

### connect_timeout_ms [long]
the timeout for connecting to InfluxDB, in milliseconds 

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## Examples
```hocon
sink {
    InfluxDB {
        url = "http://influxdb-host:8086"
        database = "test"
        measurement = "sink"
        key_time = "time"
        key_tags = ["label"]
        batch_size = 1
    }
}

```

## Changelog

### next version

- Add InfluxDB Sink Connector