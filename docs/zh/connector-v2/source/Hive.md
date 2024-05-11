# Hive

> Hive 数据源连接器

## 支持的版本

已经验证的版本 :
- 2.3.9
- 3.1.1

# 使用依赖

当你使用Spark/Flink时, 你需要保证已经与Hive进行了集成.  
当你需要使用Zeta引擎时, 你需要将这些依赖放到`$SEATUNNEL_HOME/lib/`目录中.
- `seatunnel-hadoop3-3.1.4-uber.jar`
- `hive-exec-<hive_version>.jar`
- `libfb303-0.9.3.jar`
- `hive-jdbc-<hive_version>.jar` (当设置了`hive_jdbc_url`参数, 需要使用`savemode`功能时)

## 主要特性

- [x] [批任务](../../concept/connector-v2-features.md)
- [ ] [流式任务](../../concept/connector-v2-features.md)
- [x] [精准一次](../../concept/connector-v2-features.md)

在 pollNext 调用中读取拆分分片中的所有数据。读取的分片将保存在快照中.

- [x] [schema projection](../../concept/connector-v2-features.md)
- [x] [并行读取](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片读取](../../concept/connector-v2-features.md)
- [x] 支持读取的文件类型
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json

## 数据类型映射

| Hive Data Type | SeaTunnel Data Type |
|----------------|---------------------|
| tinyint        | byte                |
| smallint       | short               |
| int            | int                 |
| bigint         | long                |
| float          | float               |
| double         | double              |
| decimal        | decimal             |
| timestamp      | local_date_time     |
| date           | local_date          |
| interval       | not supported       |
| string         | string              |
| varchar        | string              |
| char           | not supported       |
| boolean        | boolean             |
| binary         | byte array          |
| arrays         | array               |
| maps           | map                 |
| structs        | seatunnel row       |
| union          | not supported       |

## 连接器选项

|          名称          |   类型   | 是否必要 |      默认值       |                        描述                        |
|----------------------|--------|------|----------------|--------------------------------------------------|
| table_name           | string | yes  | -              | 读取的表名 例如: `db1.table1`                           |
| metastore_uri        | string | yes  | -              | Hive metastore 地址                                |
| krb5_path            | string | no   | /etc/krb5.conf | `krb5.conf`文件地址                                  |
| kerberos_principal   | string | no   | -              | The principal of kerberos authentication         |
| kerberos_keytab_path | string | no   | -              | keytab文件地址                                       |
| hdfs_site_path       | string | no   | -              | `hdfs-site.xml`文件地址                              |
| hive_site_path       | string | no   | -              | `hive-site.xml`文件地址                              |
| read_partitions      | list   | no   | -              | 需要读取的分区配置, 如果未进行配置会读取所有分区                        |
| read_columns         | list   | no   | -              | 需要抽取的字段                                          |
| compress_codec       | string | no   | none           | 文件压缩方式                                           |
| common-options       |        | no   | -              | 插件常用参数，请参考 [Source常用选项 ](common-options.md) 了解详情 |

### read_partitions

**Tips: Every partition in partitions list should have the same directory depth. For example, a hive table has two partitions: par1 and par2, if user sets it like as the following:**
**read_partitions = [par1=xxx, par1=yyy/par2=zzz], it is illegal**

### compress_codec

The details that supported as the following shown:

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc/parquet:  
  automatically recognizes the compression type, no additional settings required.

## Config Example

### Example 1: Single table

```bash

  Hive {
    table_name = "default.seatunnel_orc"
    metastore_uri = "thrift://namenode001:9083"
  }

```

### Example 2: Multiple tables

```bash

  Hive {
    tables_configs = [
        {
          table_name = "default.seatunnel_orc_1"
          metastore_uri = "thrift://namenode001:9083"
        },
        {
          table_name = "default.seatunnel_orc_2"
          metastore_uri = "thrift://namenode001:9083"
        }
    ]
  }

```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Hive Source Connector

### Next version

- [Improve] Support kerberos authentication ([3840](https://github.com/apache/seatunnel/pull/3840))
- Support user-defined partitions ([3842](https://github.com/apache/seatunnel/pull/3842))

