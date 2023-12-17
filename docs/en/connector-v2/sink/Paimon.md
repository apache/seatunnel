# Paimon

> Paimon sink connector

## Description

Write data to Apache Paimon.

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)

## Options

|      name      |  type  | required | default value |
|----------------|--------|----------|---------------|
| warehouse      | String | Yes      | -             |
| database       | String | Yes      | -             |
| table          | String | Yes      | -             |
| hdfs_site_path | String | No       | -             |

### warehouse [string]

Paimon warehouse path

### database [string]

The database you want to access

### table [String]

The table you want to access

## Examples

```hocon
sink {
  Paimon {
    warehouse = "/tmp/paimon"
    database = "default"
    table = "st_test"
  }
}
```

## Changelog

### next version

- Add Paimon Sink Connector

