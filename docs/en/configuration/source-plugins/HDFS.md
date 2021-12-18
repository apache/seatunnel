# Source plugin: HDFS

## Description

Read data from HDFS.

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| path           | string | yes      | -             |
| format         | string | no      | -             |

### path [string]

HDFS file path

### format [string]

HDFS file format

### common options [string]

Source Plugin common parameters, refer to [Source Plugin](./source-plugin.md) for details

## Example

```bash
hdfs {
    path = "/master:8020/tmp/test.csv"
    format = "csv"
    result_table_name = "test"
}
```

