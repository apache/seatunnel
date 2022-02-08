# Sink plugin : JDBC [Flink]

## Description

Write data through jdbc

## Options

| name              | type   | required | default value |
| ----------------- | ------ | -------- | ------------- |
| driver            | string | yes      | -             |
| url               | string | yes      | -             |
| username          | string | yes      | -             |
| password          | string | no       | -             |
| query             | string | yes      | -             |
| batch_size        | int    | no       | -             |
| source_table_name | string | yes      | -             |
| common-options    | string | no       | -             |

### driver [string]

Driver name, such as `com.mysql.jdbc.Driver`

### url [string]

The URL of the JDBC connection. Such as: `jdbc:mysql://localhost:3306/test`

### username [string]

username

### password [string]

password

### query [string]

Insert statement

### batch_size [int]

Number of writes per batch

### common options [string]

Sink plugin common parameters, please refer to [Sink Plugin](./sink-plugin.md) for details

## Examples

```bash
   JdbcSink {
     source_table_name = fake
     driver = com.mysql.jdbc.Driver
     url = "jdbc:mysql://localhost/test"
     username = root
     query = "insert into test(name,age) values(?,?)"
     batch_size = 2
   }
```
