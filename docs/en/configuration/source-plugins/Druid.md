# Source plugin: Druid

## Description

Read data from Apache Druid.

## Options

| name            | type           | required | default value |
| --------------- | -------------- | -------- | ------------- |
| jdbc_url        | `String`       | yes      | -             |
| datasource      | `String`       | yes      | -             |
| start_timestamp | `Long`         | no       | -             |
| end_timestamp   | `Long`         | no       | -             |
| columns         | `List<String>` | no       | `*`           |


### jdbc_url [`String`]

The URL of JDBC of Apache Druid.

### datasource [`String`]

The DataSource name in Apache Druid.

### start_timestamp [`Long`]

The start timestamp of DataSource.

### end_timestamp [`Long`]

The end timestamp of DataSource.

### columns [`List<String>`]

These columns that you want to query of DataSource.

### common options [string]

Source Plugin common parameters, refer to [Source Plugin](./source-plugin.md) for details

## Example

```bash
druid {
    jdbc_url = "jdbc:avatica:remote:url=http://localhost:8082/druid/v2/sql/avatica/"
    datasource = "wikipedia"
    start_timestamp = 1464710400
    end_timestamp = 1467302400
    columns = ["flags","page"]
}
```

