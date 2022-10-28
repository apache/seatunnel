# Iceberg

> Iceberg sink connector

## Description

Write data to Iceberg.

:::tip

Engine Supported and plugin name

* [x] Spark: Iceberg
* [ ] Flink

:::

## Options

| name                                                         | type   | required | default value |
| ------------------------------------------------------------ | ------ | -------- | ------------- |
| [path](#path)                                                | string | yes      | -             |
| [saveMode](#saveMode)                                        | string | no       | append        |
| [target-file-size-bytes](#target-file-size-bytes)            | long   | no       | -             |
| [check-nullability](#check-nullability)                      | bool   | no       | -             |
| [snapshot-property.custom-key](#snapshot-property.custom-key)| string | no       | -             |
| [fanout-enabled](#fanout-enabled)                            | bool   | no       | -             |
| [check-ordering](#check-ordering)                            | bool   | no       | -             |


Refer to [iceberg write options](https://iceberg.apache.org/docs/latest/spark-configuration/) for more configurations.

### path

Iceberg table location.

### saveMode

append or overwrite. Only these two modes are supported by iceberg. The default value is append.

### target-file-size-bytes

Overrides this table’s write.target-file-size-bytes

### check-nullability

Sets the nullable check on fields

### snapshot-property.custom-key

Adds an entry with custom-key and corresponding value in the snapshot summary
eg: snapshot-property.aaaa="bbbb"

### fanout-enabled

Overrides this table’s write.spark.fanout.enabled

### check-ordering

Checks if input schema and table schema are same

## Example

```bash
iceberg {
    path = "hdfs://localhost:9000/iceberg/warehouse/db/table"
  }
```


