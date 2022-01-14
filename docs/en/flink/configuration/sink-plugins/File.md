# Sink plugin : File [Flink]

## Description

Write data to the file system

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| format         | string | yes      | -             |
| path           | string | yes      | -             |
| write_mode     | string | no       | -             |
| common-options | string | no       | -             |

### format [string]

Currently, `csv` , `json` , and `text` are supported. The streaming mode currently only supports `text`

### path [string]

The file path is required. The `hdfs file` starts with `hdfs://` , and the `local file` starts with `file://` .

### write_mode [string]

- NO_OVERWRITE

    - No overwrite, there is an error in the path

- OVERWRITE

    - Overwrite, delete and then write if the path exists

### common options [string]

Sink plugin common parameters, please refer to [Sink Plugin](./sink-plugin.md) for details

## Examples

```bash
  FileSink {
    format = "json"
    path = "hdfs://localhost:9000/flink/output/"
    write_mode = "OVERWRITE"
  }
```
