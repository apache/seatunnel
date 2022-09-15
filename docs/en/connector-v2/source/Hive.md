# Hive

> Hive source connector

## Description

Read data from Hive.

In order to use this connector, You must ensure your spark/flink cluster already integrated hive. The tested hive version is 2.3.9.

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)

By default, we use 2PC commit to ensure `exactly-once`

- [ ] [schema projection](../../concept/connector-v2-features.md)
- [x] file format
  - [x] text
  - [x] parquet
  - [x] orc

## Options

| name                  | type   | required | default value                                                 |
|-----------------------| ------ | -------- | ------------------------------------------------------------- |
| table_name            | string | yes      | -                                                             |
| metastore_uri         | string | yes      | -                                                             |

### table_name [string]

Target Hive table name eg: db1.table1

### metastore_uri [string]

Hive metastore uri

## Example

```bash

  Hive {
    table_name = "default.seatunnel_orc"
    metastore_uri = "thrift://namenode001:9083"
  }

```
