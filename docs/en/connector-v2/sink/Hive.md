# Hive

> Hive sink connector

## Description

Write data to Hive.

In order to use this connector, You must ensure your spark/flink cluster already integrated hive. The tested hive version is 2.3.9.

## Key features

- [x] [exactly-once](key-features.md)

By default, we use 2PC commit to ensure `exactly-once`

- [ ] [schema projection](key-features.md)
- [x] file format
  - [x] text
  - [x] parquet
  - [x] orc

## Options

| name                              | type   | required | default value                                                 |
| --------------------------------- | ------ | -------- | ------------------------------------------------------------- |
| hive_table_name                   | string | yes      | -                                                             |
| hive_metastore_uris               | string | yes      | -                                                             |
| partition_by                      | array  | no       | -                                                             |
| sink_columns                      | array  | no       | When this parameter is empty, all fields are sink columns     |
| is_enable_transaction             | boolean| no       | true                                                          |
| save_mode                         | string | no       | "append"                                                      |

### hive_table_name [string]

Target Hive table name eg: db1.table1

### hive_metastore_uris [string]

Hive metastore uris

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
    hive_table_name="db1.table1"
    hive_metastore_uris="thrift://localhost:9083"
    partition_by=["age"]
    sink_columns=["name","age"]
    is_enable_transaction=true
    save_mode="append"
}

```
