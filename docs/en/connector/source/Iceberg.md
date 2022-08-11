# Iceberg

> Iceberg source connector

## Description

Read data from Iceberg.

:::tip

Engine Supported and plugin name

* [x] Spark: Iceberg
* [ ] Flink

:::

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| common-options |        | yes      | -             |
| [path](#path)  | string | yes      | -             |
| [pre_sql](#pre_sql) | string | yes | -             |
| [snapshot-id](#snapshot-id) | long | no      | -   |
| [as-of-timestamp](#as-of-timestamp) | long | no| - |


Refer to [iceberg read options](https://iceberg.apache.org/docs/latest/spark-configuration/) for more configurations.

### common-options

Source plugin common parameters, please refer to [Source Plugin](common-options.mdx) for details

### path

Iceberg table location.

### pre_sql

SQL statements queried from iceberg table. Note that the table name is `result_table_name` configuration

### snapshot-id

Snapshot ID of the table snapshot to read

### as-of-timestamp

A timestamp in milliseconds; the snapshot used will be the snapshot current at this time.

## Example

```bash
iceberg {
    path = "hdfs://localhost:9000/iceberg/warehouse/db/table"
    result_table_name = "my_source"
    pre_sql="select * from my_source where dt = '2019-01-01'"
}
```


