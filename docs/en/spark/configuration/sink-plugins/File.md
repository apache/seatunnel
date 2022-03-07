# File

> Sink plugin : File [Spark]

## Description

Output data to local or hdfs file.

## Options

| name             | type   | required | default value  |
| ---------------- | ------ | -------- | -------------- |
| options          | object | no       | -              |
| partition_by     | array  | no       | -              |
| path             | string | yes      | -              |
| path_time_format | string | no       | yyyyMMddHHmmss |
| save_mode        | string | no       | error          |
| serializer       | string | no       | json           |
| common-options   | string | no       | -              |

### options [object]

Custom parameters

### partition_by [array]

Partition data based on selected fields

### path [string]

The file path is required. The `hdfs file` starts with `hdfs://` , and the `local file` starts with `file://`,
we can add the variable `${now}` or `${uuid}` in the path, like `hdfs:///test_${uuid}_${now}.txt`, 
`${now}` represents the current time, and its format can be defined by specifying the option `path_time_format`

### path_time_format [string]

When the format in the `path` parameter is `xxxx-${now}` , `path_time_format` can specify the time format of the path, and the default value is `yyyy.MM.dd` . The commonly used time formats are listed as follows:

| Symbol | Description        |
| ------ | ------------------ |
| y      | Year               |
| M      | Month              |
| d      | Day of month       |
| H      | Hour in day (0-23) |
| m      | Minute in hour     |
| s      | Second in minute   |

See [Java SimpleDateFormat](https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html) for detailed time format syntax.

### save_mode [string]

Storage mode, currently supports `overwrite` , `append` , `ignore` and `error` . For the specific meaning of each mode, see [save-modes](https://spark.apache.org/docs/latest/sql-programming-guide.html#save-modes)

### serializer [string]

Serialization method, currently supports `csv` , `json` , `parquet` , `orc` and `text`

### common options [string]

Sink plugin common parameters, please refer to [Sink Plugin](./sink-plugin.md) for details

## Example

```bash
file {
    path = "file:///var/logs"
    serializer = "text"
}
```
