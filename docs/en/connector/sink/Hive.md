# Hive

> Hive sink connector

### Description

Write Rows to [Apache Hive](https://hive.apache.org).

:::tip

Engine Supported and plugin name

* [x] Spark: Hive
* [ ] Flink

:::

### Options

| name                                    | type          | required | default value |
| --------------------------------------- | ------------- | -------- | ------------- |
| [sql](#sql-string)                             | string        | no       | -             |
| [source_table_name](#source_table_name-string) | string        | no       | -             |
| [result_table_name](#result_table_name-string) | string        | no       | -             |
| [sink_columns](#sink_columns-string)           | string        | no       | -             |
| [save_mode](#save_mode-string)                 | string        | no       | -             |
| [partition_by](#partition_by-arraystring)           | Array[string] | no       | -             |

##### sql [string]
Hive sql：the whole insert data sql, such as `insert into/overwrite $table  select * from xxx_table `, If this option exists, other options will be ignored.

##### Source_table_name [string]

Datasource of this plugin.

##### result_table_name [string]

The output hive table name if the `sql` option doesn't specified.

##### save_mode [string]

Same with option `spark.mode` in Spark, combined with `result_table_name` if the `sql` option doesn't specified.

##### sink_columns [string]

Specify the selected fields which write to result_table_name, separated by commas, combined with `result_table_name` if the `sql` option doesn't specified.

##### partition_by [Array[string]]

Hive partition fields, combined with `result_table_name` if the `sql` option doesn't specified.

### Example

```conf
sink {
  Hive {
    sql = "insert overwrite table seatunnel.test1 partition(province) select name,age,province from myTable2"
  }
}
```

```conf
sink {
  Hive {
    source_table_name = "myTable2"
    result_table_name = "seatunnel.test1"
    save_mode = "overwrite"
    sink_columns = "name,age,province"
    partition_by = ["province"]
  }
}
```
