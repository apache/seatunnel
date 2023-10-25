# Hive

> Hive source connector

## Description

Read data from Hive.

:::tip

In order to use this connector, You must ensure your spark/flink cluster already integrated hive. The tested hive version is 2.3.9.

If you use SeaTunnel Engine, You need put seatunnel-hadoop3-3.1.4-uber.jar and hive-exec-2.3.9.jar in $SEATUNNEL_HOME/lib/ dir.
:::

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)

Read all the data in a split in a pollNext call. What splits are read will be saved in snapshot.

- [x] [schema projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)
- [x] file format
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json

## Options

|             name              |  type   | required | default value |
|-------------------------------|---------|----------|---------------|
| table_name                    | string  | yes      | -             |
| metastore_uri                 | string  | yes      | -             |
| kerberos_principal            | string  | no       | -             |
| kerberos_keytab_path          | string  | no       | -             |
| hdfs_site_path                | string  | no       | -             |
| hive_site_path                | string  | no       | -             |
| read_partitions               | list    | no       | -             |
| read_columns                  | list    | no       | -             |
| abort_drop_partition_metadata | boolean | no       | true          |
| compress_codec                | string  | no       | none          |
| common-options                |         | no       | -             |

### table_name [string]

Target Hive table name eg: db1.table1

### metastore_uri [string]

Hive metastore uri

### hdfs_site_path [string]

The path of `hdfs-site.xml`, used to load ha configuration of namenodes

### hive_site_path [string]

The path of `hive-site.xml`, used to authentication hive metastore

### read_partitions [list]

The target partitions that user want to read from hive table, if user does not set this parameter, it will read all the data from hive table.

**Tips: Every partition in partitions list should have the same directory depth. For example, a hive table has two partitions: par1 and par2, if user sets it like as the following:**
**read_partitions = [par1=xxx, par1=yyy/par2=zzz], it is illegal**

### kerberos_principal [string]

The principal of kerberos authentication

### kerberos_keytab_path [string]

The keytab file path of kerberos authentication

### read_columns [list]

The read column list of the data source, user can use it to implement field projection.

### abort_drop_partition_metadata [list]

Flag to decide whether to drop partition metadata from Hive Metastore during an abort operation. Note: this only affects the metadata in the metastore, the data in the partition will always be deleted(data generated during the synchronization process).

### compress_codec [string]

The compress codec of files and the details that supported as the following shown:

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc/parquet:  
  automatically recognizes the compression type, no additional settings required.

### common options

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details

## Example

```bash

  Hive {
    table_name = "default.seatunnel_orc"
    metastore_uri = "thrift://namenode001:9083"
  }

```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Hive Source Connector

### Next version

- [Improve] Support kerberos authentication ([3840](https://github.com/apache/seatunnel/pull/3840))
- Support user-defined partitions ([3842](https://github.com/apache/seatunnel/pull/3842))

