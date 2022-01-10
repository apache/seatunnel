# Sink plugin: Druid

## Description

Write data to Apache Druid.

## Options

| name            | type     | required | default value |
| --------------- | -------- | -------- | ------------- |
| coordinator_url | `String` | yes      | -             |
| datasource      | `String` | yes      | -             |

### coordinator_url [`String`]

The URL of Coordinator service in Apache Druid.

### datasource [`String`]

The DataSource name in Apache Druid.

## Example

```hocon
DruidSink {
  coordinator_url = "http://localhost:8081/"
  datasource = "wikipedia"
}
```
