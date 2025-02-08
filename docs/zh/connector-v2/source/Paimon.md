# Paimon

> Paimon 源连接器

## 描述

用于从 `Apache Paimon` 读取数据

## 主要功能

- [x] [批处理](../../concept/connector-v2-features.md)
- [x] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)
- [ ] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../concept/connector-v2-features.md)

## 配置选项

|          名称           |  类型  | 是否必须 | 默认值 |
|-------------------------|--------|----------|---------------|
| warehouse               | String | 是      | -             |
| catalog_type            | String | 否       | filesystem    |
| catalog_uri             | String | 否       | -             |
| database                | String | 是      | -             |
| table                   | String | 是      | -             |
| hdfs_site_path          | String | 否       | -             |
| query                   | String | 否       | -             |
| paimon.hadoop.conf      | Map    | 否       | -             |
| paimon.hadoop.conf-path | String | 否       | -             |

### warehouse [string]

Paimon warehouse 路径

### catalog_type [string]

Paimon Catalog 类型，支持 filesystem 和 hive

### catalog_uri [string]

Paimon 的 catalog uri，仅当 catalog_type 为 hive 时需要

### database [string]

需要访问的数据库

### table [string]

需要访问的表

### hdfs_site_path [string]

`hdfs-site.xml` 文件地址

### query [string]

读取表格的筛选条件，例如：`select * from st_test where id > 100`。如果未指定，则将读取所有记录。 

目前，`where` 支持`<, <=, >, >=, =, !=, or, and,is null, is not null`，其他暂不支持。 

由于 Paimon 限制，目前不支持 `Having`, `Group By` 和 `Order By`，未来版本将会支持 `projection` 和 `limit`。

注意：当 `where` 后的字段为字符串或布尔值时，其值必须使用单引号，否则将会报错。例如 `name='abc'` 或 `tag='true'`。

当前 `where` 支持的字段数据类型如下：

* string
* boolean
* tinyint
* smallint
* int
* bigint
* float
* double
* date
* timestamp

### paimon.hadoop.conf [string]

hadoop conf 属性

### paimon.hadoop.conf-path [string]

指定 'core-site.xml', 'hdfs-site.xml', 'hive-site.xml' 文件加载路径。

## Filesystems

Paimon 连接器支持向多个文件系统写入数据。目前，支持的文件系统有 `hdfs` 和 `s3`。 
如果使用 `s3` 文件系统，可以在 `paimon.hadoop.conf` 中配置`fs.s3a.access-key`、`fs.s3a.secret-key`、`fs.s3a.endpoint`、`fs.s3a.path.style.access`、`fs.s3a.aws.credentials.provider` 属性，数仓地址应该以 `s3a://` 开头。

## 示例

### 简单示例

```hocon
source {
 Paimon {
     warehouse = "/tmp/paimon"
     database = "default"
     table = "st_test"
   }
}
```

### Filter 示例

```hocon
source {
  Paimon {
    warehouse = "/tmp/paimon"
    database = "full_type"
    table = "st_test"
    query = "select c_boolean, c_tinyint from st_test where c_boolean= 'true' and c_tinyint > 116 and c_smallint = 15987 or c_decimal='2924137191386439303744.39292213'"
  }
}
```

###  S3 示例
```hocon
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  Paimon {
    warehouse = "s3a://test/"
    database = "seatunnel_namespace11"
    table = "st_test"
    paimon.hadoop.conf = {
        fs.s3a.access-key=G52pnxg67819khOZ9ezX
        fs.s3a.secret-key=SHJuAQqHsLrgZWikvMa3lJf5T0NfM5LMFliJh9HF
        fs.s3a.endpoint="http://minio4:9000"
        fs.s3a.path.style.access=true
        fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
    }
  }
}

sink {
  Console{}
}
```

### Hadoop 配置示例

```hocon
source {
  Paimon {
    catalog_name="seatunnel_test"
    warehouse="hdfs:///tmp/paimon"
    database="seatunnel_namespace1"
    table="st_test"
    query = "select * from st_test where pk_id is not null and pk_id < 3"
    paimon.hadoop.conf = {
      fs.defaultFS = "hdfs://nameservice1"
      dfs.nameservices = "nameservice1"
      dfs.ha.namenodes.nameservice1 = "nn1,nn2"
      dfs.namenode.rpc-address.nameservice1.nn1 = "hadoop03:8020"
      dfs.namenode.rpc-address.nameservice1.nn2 = "hadoop04:8020"
      dfs.client.failover.proxy.provider.nameservice1 = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
      dfs.client.use.datanode.hostname = "true"
    }
  }
}
```

### Hive catalog 示例

```hocon
source {
  Paimon {
    catalog_name="seatunnel_test"
    catalog_type="hive"
    catalog_uri="thrift://hadoop04:9083"
    warehouse="hdfs:///tmp/seatunnel"
    database="seatunnel_test"
    table="st_test3"
    paimon.hadoop.conf = {
      fs.defaultFS = "hdfs://nameservice1"
      dfs.nameservices = "nameservice1"
      dfs.ha.namenodes.nameservice1 = "nn1,nn2"
      dfs.namenode.rpc-address.nameservice1.nn1 = "hadoop03:8020"
      dfs.namenode.rpc-address.nameservice1.nn2 = "hadoop04:8020"
      dfs.client.failover.proxy.provider.nameservice1 = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
      dfs.client.use.datanode.hostname = "true"
    }
  }
}
```

## Changelog

如果要读取 paimon 表的 changelog，首先要为 Paimon 源表设置 `changelog-producer`，然后使用 SeaTunnel 流任务读取。

### Note

目前，批读取总是读取最新的快照，如需读取更完整的 changelog 数据，需使用流读取，并在将数据写入 Paimon 表之前开始流读取，为了确保顺序，流读取任务并行度应该设置为 1。

### Streaming read 示例
```hocon
env {
  parallelism = 1
  job.mode = "Streaming"
}

source {
  Paimon {
    warehouse = "/tmp/paimon"
    database = "full_type"
    table = "st_test"
  }
}

sink {
  Paimon {
    warehouse = "/tmp/paimon"
    database = "full_type"
    table = "st_test_sink"
    paimon.table.primary-keys = "c_tinyint"
  }
}
```
