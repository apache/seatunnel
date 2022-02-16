# Hdfs

> Source plugin : Hdfs [Spark]

## Description
Export data to HDFS

## Options

| name | type | required | default value |
| --- | --- | --- | --- |
| options| object | no | - |
| partition_by | array | no | - |
| path | string | yes | - |
| save_mode | string | no | error |
| serializer | string | no | json |
| common-options | string | no | - |

#### options [object]

Custom parameters

#### partition_by [array]

Partition data based on selected fields

#### path [string]

Output file path, starting with **hdfs://**

#### save_mode [string]

Storage mode, currently supports overwrite, append, ignore and error. For the specific meaning of each mode, see [save-modes](http://spark.apache.org/docs/2.2.0/sql-programming-guide.html#save-modes)

#### serializer [string]

Serialization method, currently supports csv, json, parquet, orc and text

### common options [string]

Source Plugin common parameters, refer to [Source Plugin](./source-plugin.md) for details

## Examples

```
hdfs {
    path = "hdfs:///var/logs"
    serializer = "json"
}
```