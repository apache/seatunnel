# HdfsFile

> Hdfs file source connector

## Description

Read data from hdfs file system.

## Options

| name         | type   | required | default value |
|--------------| ------ |----------|---------------|
| path         | string | yes      | -             |
| type         | string | yes      | -             |
| fs.defaultFS | string | yes      | -             |

### path [string]

The source file path.

### type [string]

File type, supported as the following file types:

`text` `csv` `parquet` `orc` `json`

### fs.defaultFS [string]

Hdfs cluster address.

## Example

```hcon

HdfsFile {
  path = "/apps/hive/demo/student"
  type = "parquet"
  fs.defaultFS = "hdfs://namenode001"
}

```