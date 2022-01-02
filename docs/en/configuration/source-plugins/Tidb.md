# Source plugin: Tidb

## Description

Read data from Tidb.

## Env Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| spark.tispark.pd.addresses       | string | yes      | -             |
| spark.sql.extensions        | string | yes      | org.apache.spark.sql.TiExtensions         |

### spark.tispark.pd.addresses [string]

your pd servers

### spark.sql.extensions [string]

default value : org.apache.spark.sql.TiExtensions

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| database       | string | yes      | -             |
| pre_sql        | string | yes      | -         |

### database [string]

Tidb database

### pre_sql [string]

sql script

### common options [string]

Source Plugin common parameters, refer to [Source Plugin](./source-plugin.md) for details

## Example

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

