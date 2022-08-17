# LocalFile

> Local file source connector

## Description

Read data from local file system.

## Options

| name         | type   | required | default value |
|--------------| ------ |----------|---------------|
| path         | string | yes      | -             |
| type         | string | yes      | -             |

### path [string]

The source file path.

### type [string]

File type, supported as the following file types:

`text` `csv` `parquet` `orc` `json`

## Example

```hcon

LocalFile {
  path = "/apps/hive/demo/student"
  type = "parquet"
}

```