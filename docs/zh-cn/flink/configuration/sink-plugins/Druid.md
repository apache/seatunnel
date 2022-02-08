# Sink plugin: Druid

## 描述

将数据写入 Druid

## 配置

| name             | type     | required | default value |
| ---------------- | -------- | -------- | ------------- |
| coordinator_url  | `String` | yes      | -             |
| datasource       | `String` | yes      | -             |
| timestamp_column | `String` | no       | timestamp     |
| timestamp_format | `String` | no       | auto          |
| parallelism      | `Int`    | no       | -             |

### coordinator_url [`String`]

Druid 中 Coordinator Server 的 URL

### datasource [`String`]

Druid 中的数据源名称

### timestamp_column [`String`]

Druid 中的时间戳格式，默认值为：`timestamp`

### timestamp_format [`String`]

Druid 中的时间戳格式，默认值为： `auto`, 也可以是:

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

### parallelism [`Int`]

DruidSink 的单个操作符的并行性

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
