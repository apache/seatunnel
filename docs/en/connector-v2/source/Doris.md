# Doris

> Doris source connector

## Description

Used to read data from Doris. Currently, only supports Query with Batch Mode.

 The tested doris version is 1.0.0.

## Options

| name                     | type    | required | default value |
|--------------------------|---------|----------|---------------|
| doris_be_address             | string  | yes      | -             |
| username               | string  | yes      | -             |
| password               | string  | yes      | -             |
| select_sql               | string  | yes      | -             |
| database               | string  | yes      | -             |

### doris_be_address [string]

The address of doris fe,such as '192.168.88.120:9030'.

### username [string]

The login user name of FE.

### password [string]

The login password of FE.

### select_sql [string]

SQL to get data from Doris .

### database [string]

database to get data from Doris .

## Examples

```hocon
source {
 DorisSource {
    doris_be_address="192.168.88.120:9030"
      username="root"
      password="xxxx"
     select_sql="select * from example_db.table_hashlyh2"
     database="example_db"
    }

}
```