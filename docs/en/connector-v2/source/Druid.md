# Druid

> Druid source connector

## Description

Read data from Apache Druid.

## Options

| name       | type           | required | default value |
| ---------- | -------------- | -------- | ------------- |
| url        | `String`       | yes      | -             |
| datasource | `String`       | yes      | -             |
| start_date | `String`       | no       | -             |
| end_date   | `String`       | no       | -             |
| columns    | `List<String>` | no       | `*`           |

### url [`String`]

The URL of JDBC of Apache Druid.

### datasource [`String`]

The DataSource name in Apache Druid.

### start_date [`String`]

The start date of DataSource, for example, `'2016-06-27'`, `'2016-06-27 00:00:00'`, etc.

### end_date [`String`]

The end date of DataSource, for example, `'2016-06-28'`, `'2016-06-28 00:00:00'`, etc.

### columns [`List<String>`]

These columns that you want to write  of DataSource.

### common options [string]

Source Plugin common parameters, refer to [Source Plugin](common-options.mdx) for details


## Example

```hocon
DruidSource {
  url = "jdbc:avatica:remote:url=http://localhost:8082/druid/v2/sql/avatica/"
  datasource = "wikipedia"
  start_date = "2016-06-27 00:00:00"
  end_date = "2016-06-28 00:00:00"
  columns = ["flags","page"]
}
```
