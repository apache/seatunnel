# Sink plugin: Delta [Spark]

## Description

Write Rows to  Delta Lake.

## Options

| name | type | required | default value | engine |
| --- | --- | --- | --- | --- |
| delta.sink.path | string | yes | - | Spark |
| save_mode	 | string | no | append | Spark |
| replaceWhere	 | string | no | - | Spark |

[More Delta](https://docs.delta.io/0.6.0/delta-batch.html#write-to-a-table)

### delta.sink.path [string]

Base path on lake storage, under which all the table data is stored. Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). 

### save_mode [string]
It can be `overwrite` and `append`. 
When use `overwrite`, will replace all of the data in a table.
When use `append`, will append data in a table.

### replaceWhere [string]
It can be a condition like `date >= '2017-01-01' AND date <= '2017-01-31'` and must use with `overwrite`  save_mode. 
It will selectively overwrite only the data that matches predicates over partition column.

## Examples

```bash
Delta {
    delta.sink.path = "hdfs:///tmp/table1"
}
```
