# Paimon

> Paimon 数据连接器

## 描述

Apache Paimon数据连接器。支持cdc写以及自动建表。

## 主要特性

- [x] [exactly-once](../../concept/connector-v2-features.md)

## 连接器选项

|             名称              | 类型  | 是否必须 |             默认值              |                                                描述                                                 |
|-----------------------------|-----|------|------------------------------|---------------------------------------------------------------------------------------------------|
| warehouse                   | 字符串 | 是    | -                            | Paimon warehouse路径                                                                                |
| database                    | 字符串 | 是    | -                            | 数据库名称                                                                                             |
| table                       | 字符串 | 是    | -                            | 表名                                                                                                |
| hdfs_site_path              | 字符串 | 否    | -                            | hdfs-site.xml文件路径                                                                                 |
| schema_save_mode            | 枚举  | 否    | CREATE_SCHEMA_WHEN_NOT_EXIST | Schema保存模式                                                                                        |
| data_save_mode              | 枚举  | 否    | APPEND_DATA                  | 数据保存模式                                                                                            |
| paimon.table.primary-keys   | 字符串 | 否    | -                            | 主键字段列表，联合主键使用逗号分隔(注意：分区字段需要包含在主键字段中)                                                              |
| paimon.table.partition-keys | 字符串 | 否    | -                            | 分区字段列表，多字段使用逗号分隔                                                                                  |
| paimon.table.write-props    | Map | 否    | -                            | Paimon表初始化指定的属性, [参考](https://paimon.apache.org/docs/0.6/maintenance/configurations/#coreoptions) |
| paimon.hadoop.conf          | Map | 否    | -                            | Hadoop配置文件属性信息                                                                                    |
| paimon.hadoop.conf-path     | 字符串 | 否    | -                            | Hadoop配置文件目录，用于加载'core-site.xml', 'hdfs-site.xml', 'hive-site.xml'文件配置                            |

## 示例

### 单表

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    base-url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    table-names = ["seatunnel.role"]
  }
}

transform {
}

sink {
  Paimon {
    catalog_name="seatunnel_test"
    warehouse="file:///tmp/seatunnel/paimon/hadoop-sink/"
    database="seatunnel"
    table="role"
  }
}
```

### 单表(指定hadoop HA配置和kerberos配置)

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    base-url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    table-names = ["seatunnel.role"]
  }
}

transform {
}

sink {
  Paimon {
    catalog_name="seatunnel_test"
    warehouse="hdfs:///tmp/seatunnel/paimon/hadoop-sink/"
    database="seatunnel"
    table="role"
    paimon.hadoop.conf = {
      fs.defaultFS = "hdfs://nameservice1"
      dfs.nameservices = "nameservice1"
      dfs.ha.namenodes.nameservice1 = "nn1,nn2"
      dfs.namenode.rpc-address.nameservice1.nn1 = "hadoop03:8020"
      dfs.namenode.rpc-address.nameservice1.nn2 = "hadoop04:8020"
      dfs.client.failover.proxy.provider.nameservice1 = "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
      dfs.client.use.datanode.hostname = "true"
      security.kerberos.login.principal = "your-kerberos-principal"
      security.kerberos.login.keytab = "your-kerberos-keytab-path"
    }
  }
}
```

### 指定paimon的写属性的单表

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    base-url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    table-names = ["seatunnel.role"]
  }
}

sink {
  Paimon {
    catalog_name="seatunnel_test"
    warehouse="file:///tmp/seatunnel/paimon/hadoop-sink/"
    database="seatunnel"
    table="role"
    paimon.table.write-props = {
        bucket = 2
        file.format = "parquet"
    }
    paimon.table.partition-keys = "dt"
    paimon.table.primary-keys = "pk_id,dt"
  }
}
```

### 多表

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    base-url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    table-names = ["seatunnel.role","seatunnel.user","galileo.Bucket"]
  }
}

transform {
}

sink {
  Paimon {
    catalog_name="seatunnel_test"
    warehouse="file:///tmp/seatunnel/paimon/hadoop-sink/"
    database="${database_name}"
    table="${table_name}"
  }
}
```

