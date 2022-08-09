# Tidb

> TiDB source connector

### Description

Read data from Tidb.

:::tip

Engine Supported and plugin name

* [x] Spark: Tidb
* [ ] Flink

:::

### Env Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| [spark.tispark.pd.addresses](#spark.tispark.pd.addresses-string)       | string | yes      | -             |
| [spark.sql.extensions](#spark.sql.extensions-string)        | string | yes      | org.apache.spark.sql.TiExtensions         |

##### spark.tispark.pd.addresses [string]

your pd servers

##### spark.sql.extensions [string]

default value : org.apache.spark.sql.TiExtensions

### Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| [database](#database-string)       | string | yes      | -             |
| [pre_sql](#pre_sql-string)        | string | yes      | -         |

##### database [string]

Tidb database

##### pre_sql [string]

sql script

##### common options [string]

Source Plugin common parameters, refer to [Source Plugin](common-options.mdx) for details

### Example

```bash
env {
    spark.tispark.pd.addresses = "192.168.0.1:2379"
    spark.sql.extensions = "org.apache.spark.sql.TiExtensions"
}

source {
    tidb {
        database = "test"
        pre_sql = "select * from table1"
    }
}

```

