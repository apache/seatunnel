# File

> Source plugin : File [Flink]

## Description

Read data from the file system

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| format.type    | string | yes      | -             |
| path           | string | yes      | -             |
| schema         | string | yes      | -             |
| common-options | string | no       | -             |
| parallelism    | int    | no       | -             |

### format.type [string]

The format for reading files from the file system, currently supports `csv` , `json` , `parquet` , `orc` and `text` .

### path [string]

The file path is required. The `hdfs file` starts with `hdfs://` , and the `local file` starts with `file://` .

### schema [string]

- csv

    - The `schema` of `csv` is a string of `jsonArray` , such as `"[{\"type\":\"long\"},{\"type\":\"string\"}]"` , this can only specify the type of the field , The field name cannot be specified, and the common configuration parameter `field_name` is generally required.

- json

    - The `schema` parameter of `json` is to provide a `json string` of the original data, and the `schema` can be automatically generated, but the original data with the most complete content needs to be provided, otherwise the fields will be lost.

- parquet

    - The `schema` of `parquet` is an `Avro schema string` , such as `{\"type\":\"record\",\"name\":\"test\",\"fields\":[{\"name\" :\"a\",\"type\":\"int\"},{\"name\":\"b\",\"type\":\"string\"}]}` .

- orc

    - The `schema` of `orc` is the string of `orc schema` , such as `"struct<name:string,addresses:array<struct<street:string,zip:smallint>>>"` .

- text

    - The `schema` of `text` can be filled with `string` .

### common options [string]

Source plugin common parameters, please refer to [Source Plugin](./source-plugin.md) for details

### parallelism [`Int`]

The parallelism of an individual operator, for FileSource

## Examples

```bash
  FileSource{
    path = "hdfs://localhost:9000/input/"
    source_format = "json"
    schema = "{\"data\":[{\"a\":1,\"b\":2},{\"a\":3,\"b\":4}],\"db\":\"string\",\"q\":{\"s\":\"string\"}}"
    result_table_name = "test"
  }
```
