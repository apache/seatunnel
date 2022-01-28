# Sink plugin: Druid

## Description

Write data to Apache Druid.

## Options

| name             | type     | required | default value |
| ---------------- | -------- | -------- | ------------- |
| coordinator_url  | `String` | yes      | -             |
| datasource       | `String` | yes      | -             |
| timestamp_column | `String` | no       | timestamp     |
| timestamp_format | `String` | no       | auto          |

### coordinator_url [`String`]

The URL of Coordinator service in Apache Druid.

### datasource [`String`]

The DataSource name in Apache Druid.

### timestamp_column [`String`]

The timestamp column name in Apache Druid, the default value is `timestamp`.

### timestamp_format [`String`]

The timestamp format in Apache Druid, the default value is `auto`, it could be:

- `iso`
  - ISO8601 with 'T' separator, like "2000-01-01T01:02:03.456"

- `posix`
  - seconds since epoch

- `millis`
  - milliseconds since epoch

- `micro`
  - microseconds since epoch

- `nano`
  - nanoseconds since epoch

- `auto`
  - automatically detects ISO (either 'T' or space separator) or millis format

- any [Joda DateTimeFormat](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html) string

## Example

### Simple

```hocon
DruidSink {
  coordinator_url = "http://localhost:8081/"
  datasource = "wikipedia"
}
```

### Specified timestamp column and format

```hocon
DruidSink {
  coordinator_url = "http://localhost:8081/"
  datasource = "wikipedia"
  timestamp_column = "timestamp"
  timestamp_format = "auto"
}
```
