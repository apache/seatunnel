# Hudi

> Hudi sink connector

## Description

Write Rows to a Hudi.

:::tip

Engine Supported and plugin name

* [x] Spark: Hudi
* [ ] Flink

:::

## Options

| name | type | required | default value | engine |
| --- | --- | --- | --- | --- |
| hoodie.base.path | string | yes | - | Spark |
| hoodie.table.name | string | yes | - | Spark |
| save_mode	 | string | no | append | Spark |

[More hudi Configurations](https://hudi.apache.org/docs/configurations/#Write-Options)

### hoodie.base.path [string]

Base path on lake storage, under which all the table data is stored. Always prefix it explicitly with the storage scheme (e.g hdfs://, s3:// etc). Hudi stores all the main meta-data about commits, savepoints, cleaning audit logs etc in .hoodie directory under this base path directory.

### hoodie.table.name [string]

Table name that will be used for registering with Hive. Needs to be same across runs.

## Examples

```bash
hudi {
    hoodie.base.path = "hdfs://"
    hoodie.table.name = "seatunnel_hudi"
}
```
