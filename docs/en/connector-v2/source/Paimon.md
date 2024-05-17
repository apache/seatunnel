# Paimon

> Paimon source connector

## Description

Read data from Apache Paimon.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

|          name          |  type  | required | default value |
|------------------------|--------|----------|---------------|
| warehouse              | String | Yes      | -             |
| database               | String | Yes      | -             |
| table                  | String | Yes      | -             |
| hdfs_site_path         | String | No       | -             |
| paimon.read.filter.sql | String | No       | -             |

### warehouse [string]

Paimon warehouse path

### database [string]

The database you want to access

### table [string]

The table you want to access

### hdfs_site_path [string]

The file path of `hdfs-site.xml`

### paimon.read.filter.sql [string]

The filter condition of the table read. For example: `select * from st_test where id > 100`. If not specified, all rows are read. Currently, where conditions only support <, <=, >, >=, =, !=, or, and,is null, is not null, and others are not supported.</br>
Note: When the field after the where condition is a string or boolean value, its value must be enclosed in single quotes, otherwise an error will be reported. `For example: name='abc' or tag='true'`
The field data types currently supported by where conditions are as follows::
- string
- boolean
- tinyint
- smallint
- int
- bigint
- float
- double
- decimal
- date
- timestamp

## Examples

### Simple example

```hocon
source {
 Paimon {
     warehouse = "/tmp/paimon"
     database = "default"
     table = "st_test"
   }
}
```

### Filter example

```hocon
source {
  Paimon {
    warehouse = "/tmp/paimon"
    database = "full_type"
    table = "st_test"
    paimon.read.filter.sql = "select * from st_test where c_boolean= 'true' and c_tinyint > 116 and c_smallint = 15987 or c_decimal='2924137191386439303744.39292213'"
  }
}
```

## Changelog

### next version

- Add Paimon Source Connector

