# Sink plugin: Hive

### Description

Write Rows to [Apache Hive](https://hive.apache.org).

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
Hive sql：insert into/overwrite $table  select * from xxx_table  

If this option exists, other options will be ignored

##### Source_table_name [string]

Datasource of this plugin.

##### result_table_name [string]

The output hive table name.

##### save_mode [string]

Same with option `spark.mode` in Spark.

##### sink_columns [string]

Select the required fields in source_table_name and store them in result_table_name, separated by commas.

##### partition_by [Array[string]]

Hive partition fields

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
