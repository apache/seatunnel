# Sink plugin: InfluxDB

## Description

Write data to InfluxDB.

## Options

| name        | type           | required | default value |
| ----------- | -------------- | -------- | ------------- |
| server_url  | `String`       | yes      | -             |
| database    | `String`       | yes      | -             |
| measurement | `String`       | yes      | -             |
| tags        | `List<String>` | yes      | -             |
| fields      | `List<String>` | yes      | -             |

### server_url [`String`]

The URL of InfluxDB Server.

### datasource [`String`]

The DataSource name in InfluxDB.

### measurement [`String`]

The Measurement name in InfluxDB.

### tags [`List<String>`]

The list of Tag in InfluxDB.

### fields [`List<String>`]

The list of Field in InfluxDB.

## Example

```hocon
InfluxDbSink {
  server_url = "http://127.0.0.1:8086/"
  database = "influxdb"
  measurement = "m"
  tags = ["country", "city"]
  fields = ["count"]
}
```
