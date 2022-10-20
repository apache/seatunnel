# Hudi

> Hudi source connector

## Description

Used to read data from Hudi. Currently, only supports hudi cow table and Snapshot Query with Batch Mode.

In order to use this connector, You must ensure your spark/flink cluster already integrated hive. The tested hive version is 2.3.9.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)

Currently, only supports hudi cow table and Snapshot Query with Batch Mode

- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name                    | type    | required | default value |
| ----------------------- | ------- | -------- | ------------- |
| table.path              | string  | yes      | -             |
| table.type              | string  | yes      | -             |
| conf.files              | string  | yes      | -             |
| use.kerberos            | boolean | no       | false         |
| kerberos.principal      | string  | no       | -             |
| kerberos.principal.file | string  | no       | -             |
| common-options          |         | no       | -             |

### table.path [string]

`table.path` The hdfs root path of hudi table,such as 'hdfs://nameserivce/data/hudi/hudi_table/'.

### table.type [string]

`table.type` The type of hudi table. Now we only support 'cow', 'mor' is not support yet.

### conf.files [string]

`conf.files` The environment conf file path list(local path), which used to init hdfs client to read hudi table file. The example is '/home/test/hdfs-site.xml;/home/test/core-site.xml;/home/test/yarn-site.xml'.

### use.kerberos [boolean]

`use.kerberos` Whether to enable Kerberos, default is false.

### kerberos.principal [string]

`kerberos.principal` When use kerberos, we should set kerberos princal such as 'test_user@xxx'.

### kerberos.principal.file [string]

`kerberos.principal.file` When use kerberos,  we should set kerberos princal file such as '/home/test/test_user.keytab'.

### common options 

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

## Examples

```hocon
source {

  Hudi {
    table.path = "hdfs://nameserivce/data/hudi/hudi_table/"
    table.type = "cow"
    conf.files = "/home/test/hdfs-site.xml;/home/test/core-site.xml;/home/test/yarn-site.xml"
    use.kerberos = true
    kerberos.principal = "test_user@xxx"
    kerberos.principal.file = "/home/test/test_user.keytab"
  }

}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Hudi Source Connector
