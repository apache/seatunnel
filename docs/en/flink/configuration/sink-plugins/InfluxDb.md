# Sink plugin: InfluxDB

## Description

Write data to InfluxDB.

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

### server_url [`String`]

The URL of InfluxDB Server.

### username [`String`]

The username of InfluxDB Server.

### password [`String`]

The password of InfluxDB Server.

### datasource [`String`]

The DataSource name in InfluxDB.

### measurement [`String`]

The Measurement name in InfluxDB.

### tags [`List<String>`]

The list of Tag in InfluxDB.

### fields [`List<String>`]

The list of Field in InfluxDB.

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
