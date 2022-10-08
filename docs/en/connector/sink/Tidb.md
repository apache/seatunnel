# TiDB

> TiDB sink connector

### Description

Write data to TiDB.

:::tip

Engine Supported and plugin name

* [x] Spark: TiDB
* [ ] Flink

:::

### Env Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| [spark.tispark.pd.addresses](#spark.tispark.pd.addresses-string)       | string | yes      | -             |
| [spark.sql.extensions](#spark.sql.extensions-string)        | string | yes      | org.apache.spark.sql.TiExtensions         |

##### spark.tispark.pd.addresses [string]

TiDB Pd Address

##### spark.sql.extensions [string]

Spark Sql Extensions

### Options

| name             | type   | required | default value |
|------------------| ------ |----------|---------------|
| [addr](#addr-string)              | string | yes      | -             |
| [port](#port-string)              | string | yes      | -             |
| [user](#user-string)             | string | yes      | -             |
| [password](#password-string)         | string | yes      | -             |
| [table](#table-string)            | string | yes      | -             |
| [database](#database-string)        | string | yes       |        |

##### addr [string]

TiDB address, which currently only supports one instance

##### port [string]

TiDB port

##### user [string]

TiDB user

##### password [string]

TiDB password

##### table [string]

TiDB table name

##### database [string]

TiDB database name

##### options

Refer to [TiSpark Configurations](https://github.com/pingcap/tispark/blob/v2.4.1/docs/datasource_api_userguide.md)

### Examples

```bash
env {
    spark.tispark.pd.addresses = "127.0.0.1:2379"
    spark.sql.extensions = "org.apache.spark.sql.TiExtensions"
}

tidb {
    addr = "127.0.0.1",
    port = "4000"
    database = "database",
    table = "tableName",
    user = "userName",
    password = "***********"
}
```
