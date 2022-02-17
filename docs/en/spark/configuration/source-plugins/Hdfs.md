# Hdfs

> Source plugin : Hdfs [Spark]

## Description
read data from HDFS

## Options

| name | type | required | default value |
| --- | --- | --- | --- |
| format | string | no | json |
| path | string | yes | - |
| common-options| string | yes | - |

##### format [string]
Format for reading files from HDFS, currently supports text, parquet, json, orc, csv.

##### path [string]
Hadoop cluster file path, starting with hdfs://

### common options [string]

Source plugin common parameters, please refer to [Source Plugin](./source-plugin.md) for details

## Examples

```
hdfs {
    path = "hdfs:///var/logs"
    result_table_name = "access_log"
}
```