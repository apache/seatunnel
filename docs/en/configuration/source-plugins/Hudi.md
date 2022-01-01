# Source plugin: Hudi

## Description

Read data from Hudi.

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| [hoodie.datasource.read.paths](#hoodiedatasourcereadpaths-string)           | string | yes      | -             |

Refer to [hudi read options](https://hudi.apache.org/docs/configurations/#Read-Options) for configurations.

### hoodie.datasource.read.paths [string]

Comma separated list of file paths to read within a Hudi table.

## Example

```bash
hudi {
    hoodie.datasource.read.paths = "hdfs://"
}
```
