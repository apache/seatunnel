# File

> Source plugin : File [Spark]

## Description
read data from local or hdfs file.

## Options

| name | type | required | default value |
| --- | --- | --- | --- |
| format | string | no | json |
| path | string | yes | - |
| common-options| string | yes | - |

##### format [string]
Format for reading files, currently supports text, parquet, json, orc, csv.

##### path [string]
- If read data from hdfs , the file path should start with `hdfs://`  
- If read data from local , the file path should start with `file://`

### common options [string]

Source plugin common parameters, please refer to [Source Plugin](./source-plugin.md) for details

## Examples

```
file {
    path = "hdfs:///var/logs"
    result_table_name = "access_log"
}
```

```
file {
    path = "file:///var/logs"
    result_table_name = "access_log"
}
```
