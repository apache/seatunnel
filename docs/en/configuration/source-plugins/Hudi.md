# Source plugin: Hudi

## Description

Read data from Hudi.

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| [hoodie.datasource.read.paths](#hoodie.datasource.read.paths-string)           | string | yes      | -             |

[More hudi Configurations](https://hudi.apache.org/docs/configurations/#Read-Options)

### hoodie.datasource.read.paths [string]

Comma separated list of file paths to read within a Hudi table.

## Example

```bash
hudi {
    hoodie.datasource.read.paths = "hdfs://"
}
```
