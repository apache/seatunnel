# Sink plugin: Delta [Spark]

## Description

Write Rows to  Delta Lake.

## Options

| name | type | required | default value | engine |
| --- | --- | --- | --- | --- |
| delta.sink.path | string | yes | - | Spark |
| save_mode	 | string | no | append | Spark |

[More Delta](https://docs.delta.io/0.6.0/delta-batch.html#write-to-a-table)

### delta.sink.path [string]

Base path on lake storage, under which all the table data is stored. Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). 


## Examples

```bash
Delta {
    delta.sink.path = "hdfs:///tmp/table1"
}
```
