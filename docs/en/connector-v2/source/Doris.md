# Doris

> Doris source connector

## Description

Used to read data from Doris.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Options

|               name               |  type  | required | default value |
|----------------------------------|--------|----------|---------------|
| fenodes                          | string | yes      | -             |
| username                         | string | yes      | -             |
| password                         | string | yes      | -             |
| table.identifier                 | string | yes      | -             |
| schema                           | config | yes      | -             |
| doris.filter.query               | string | no       | -             |
| doris.batch.size                 | int    | no       | 1024          |
| doris.request.query.timeout.s    | int    | no       | 3600          |
| doris.exec.mem.limit             | long   | no       | 2147483648    |
| doris.request.retries            | int    | no       | 3             |
| doris.request.read.timeout.ms    | int    | no       | 30000         |
| doris.request.connect.timeout.ms | int    | no       | 30000         |

### fenodes [string]

`Doris` FE address, the format is `"fe_host:fe_http_port"`

### username [string]

`Doris` user username

### password [string]

`Doris` user password

### table.identifier [string]

The name of Doris database and table , the format is `"databases.tablename"`

### schema [config]

#### fields [Config]

The schema of the doris that you want to generate

e.g.

```
schema {
    fields {
       F_INT = "INT"
       F_BIGINT = "BIGINT"
       F_TINYINT = "TINYINT"
       F_SMALLINT = "SMALLINT"
       F_DECIMAL = "DECIMAL(18,6)"
       F_BOOLEAN = "BOOLEAN"
       F_DOUBLE = "DOUBLE"
       F_FLOAT = "FLOAT"
       F_CHAR = "String"
       F_VARCHAR_11 = "String"
       F_STRING = "String"
       F_DATETIME_P = "Timestamp"
       F_DATETIME = "Timestamp"
       F_DATE = "DATE" 
    }
  }
```

### doris.filter.query [string]

Data filtering in doris. the format is "field = value".

e.g.

```
"f_int = 1024"
```
## Example

```
source{
  Doris {
      fenodes = "doris_e2e:8030"
      username = root
      password = ""
      table.identifier = "e2e_source.doris_e2e_table"
      schema {
            fields {
            F_ID = "BIGINT"
            F_INT = "INT"
            F_BIGINT = "BIGINT"
            F_TINYINT = "TINYINT"
            F_SMALLINT = "SMALLINT"
            F_DECIMAL = "DECIMAL(18,6)"
            F_BOOLEAN = "BOOLEAN"
            F_DOUBLE = "DOUBLE"
            F_FLOAT = "FLOAT"
            F_CHAR = "String"
            F_VARCHAR_11 = "String"
            F_STRING = "String"
            F_DATETIME_P = "Timestamp"
            F_DATETIME = "Timestamp"
            F_DATE = "DATE"
            }
      }
  }
}
```
