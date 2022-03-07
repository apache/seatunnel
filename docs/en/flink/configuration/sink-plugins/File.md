# File

> Sink plugin : File [Flink]

## Description

Write data to the file system

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| format         | string | yes      | -             |
| path           | string | yes      | -             |
| path_time_format | string | no       | yyyyMMddHHmmss |
| write_mode     | string | no       | -             |
| common-options | string | no       | -             |
| parallelism    | int    | no       | -             |


### format [string]

Currently, `csv` , `json` , and `text` are supported. The streaming mode currently only supports `text`

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

### write_mode [string]

- NO_OVERWRITE

    - No overwrite, there is an error in the path

- OVERWRITE

    - Overwrite, delete and then write if the path exists

### common options [string]

Sink plugin common parameters, please refer to [Sink Plugin](./sink-plugin.md) for details

### parallelism [`Int`]

The parallelism of an individual operator, for FileSink


## Examples

```bash
  FileSink {
    format = "json"
    path = "hdfs://localhost:9000/flink/output/"
    write_mode = "OVERWRITE"
  }
```
