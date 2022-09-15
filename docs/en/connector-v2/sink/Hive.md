# Hive

> Hive sink connector

## Description

Write data to Hive.

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
| partition_by          | array  | no       | -                                                             |
| sink_columns          | array  | no       | When this parameter is empty, all fields are sink columns     |
| is_enable_transaction | boolean| no       | true                                                          |
| save_mode             | string | no       | "append"                                                      |

### table_name [string]

Target Hive table name eg: db1.table1

### metastore_uri [string]

Hive metastore uri

### partition_by [array]

Partition data based on selected fields

### sink_columns [array]

Which columns need be write to hive, default value is all of the columns get from `Transform` or `Source`.
The order of the fields determines the order in which the file is actually written.

### is_enable_transaction [boolean]

If `is_enable_transaction` is true, we will ensure that data will not be lost or duplicated when it is written to the target directory.

Only support `true` now.

### save_mode [string]

Storage mode, we need support `overwrite` and `append`. `append` is now supported.

Streaming Job not support `overwrite`.

## Example

```bash

  Hive {
    table_name = "default.seatunnel_orc"
    metastore_uri = "thrift://namenode001:9083"
  }

```
