# InfluxDB

> InfluxDB sink connector

## Description

Write data to InfluxDB.

:::tip

Engine Supported and plugin name

* [ ] Spark
* [x] Flink: InfluxDB

:::

## Options

| name        | type           | required | default value |
| ----------- | -------------- | -------- | ------------- |
| server_url  | `String`       | yes      | -             |
| username    | `String`       | no       | -             |
| password    | `String`       | no       | -             |
| database    | `String`       | yes      | -             |
| measurement | `String`       | yes      | -             |
| tags        | `List<String>` | yes      | -             |
| fields      | `List<String>` | yes      | -             |
| parallelism | `Int`          | no       | -             |

### server_url [`String`]

The URL of InfluxDB Server.

### username [`String`]

The username of InfluxDB Server.

### password [`String`]

The password of InfluxDB Server.

### database [`String`]

The database name in InfluxDB.

### measurement [`String`]

The Measurement name in InfluxDB.

### tags [`List<String>`]

The list of Tag in InfluxDB.

### fields [`List<String>`]

The list of Field in InfluxDB.

### parallelism [`Int`]

The parallelism of an individual operator, for InfluxDbSink


## Example

### Simple

```hocon
InfluxDbSink {
  server_url = "http://127.0.0.1:8086/"
  database = "influxdb"
  measurement = "m"
  tags = ["country", "city"]
  fields = ["count"]
}
```

### Auth

```hocon
InfluxDbSink {
  server_url = "http://127.0.0.1:8086/"
  username = "admin"
  password = "password"
  database = "influxdb"
  measurement = "m"
  tags = ["country", "city"]
  fields = ["count"]
}
```
