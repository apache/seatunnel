# InfluxDB

> InfluxDB sink connector

## Description

Write data to InfluxDB.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)


## Options

| name                        | type     | required | default value               |
|-----------------------------|----------|----------|-----------------------------|
| url                         | string   | yes      | -                           |
| fields                      | config   | yes      | -                           |
| database                    | string   | yes      |                             |
| measurement                 | string   | yes      |                             |
| username                    | string   | no       | -                           |
| password                    | string   | no       | -                           |
| keyTime                     | string   | yes      | processing                  |
| keyTags                     | array    | no       | exclude `field` & `keyTime` |
| batch_size                  | int      | no       | 1024                        |
| batch_interval_ms           | int      | no       | -                           |
| max_retries                 | int      | no       | -                           |
| retry_backoff_multiplier_ms | int      | no       | -                           |
| connect_timeout_ms          | long     | no       | 15000                       |

### url
the url to connect to influxDB e.g.
``` 
http://influxdb-host:8086
```

### sql [string]
The query sql used to search data

```
select name,age from test
```

### fields [string]

the fields of the InfluxDB when you select

the field type is SeaTunnel field type `org.apache.seatunnel.api.table.type.SqlType`

e.g.

```
fields{
    name=STRING
    age=INT
    }
```

### database [string]

The `influxDB` database

### username [string]

the username of the influxDB when you select

### password [string]

the password of the influxDB when you select

### keyTime [string]

Specify field-name of the `influxDB` timestamp in SeaTunnelRow. If not specified, use processing-time as timestamp

### keyTags [array]

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

## Examples
```hocon
sink {
    InfluxDB {
        url = "http://influxdb-host:8086"
        database = "test"
        measurement = "sink"
        batch_size = 1
    }
}

```