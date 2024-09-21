# Paimon

> Paimon 数据连接器

## 描述

Apache Paimon数据连接器。支持cdc写以及自动建表。

## 支持的数据源信息

|  数据源   |    依赖     |                                   Maven                                   |
|--------|-----------|---------------------------------------------------------------------------|
| Paimon | hive-exec | [Download](https://mvnrepository.com/artifact/org.apache.hive/hive-exec)  |
| Paimon | libfb303  | [Download](https://mvnrepository.com/artifact/org.apache.thrift/libfb303) |

## 数据源依赖

> 为了兼容不同版本的Hadoop和Hive，在项目pom文件中Hive -exec的作用域为provided，所以如果您使用Flink引擎，首先可能需要将以下Jar包添加到<FLINK_HOME>/lib目录下，如果您使用Spark引擎并与Hadoop集成，则不需要添加以下Jar包。

```
hive-exec-xxx.jar
libfb303-xxx.jar
```

> 有些版本的hive-exec包没有libfb303-xxx.jar，所以您还需要手动导入Jar包。

## 主要特性

- [x] [exactly-once](../../concept/connector-v2-features.md)

## 连接器选项

| 名称                        | 类型  | 是否必须 | 默认值                        | 描述                                                                                                |
|-----------------------------|-------|----------|------------------------------|---------------------------------------------------------------------------------------------------|
| warehouse                   | 字符串 | 是       | -                            | Paimon warehouse路径                                                                                |
| catalog_type                | 字符串 | 否       | filesystem                   | Paimon的catalog类型，目前支持filesystem和hive                                                              |
| catalog_uri                 | 字符串 | 否       | -                            | Paimon catalog的uri，仅当catalog_type为hive时需要配置                                                       |
| database                    | 字符串 | 是       | -                            | 数据库名称                                                                                             |
| table                       | 字符串 | 是       | -                            | 表名                                                                                                |
| hdfs_site_path              | 字符串 | 否       | -                            | hdfs-site.xml文件路径                                                                                 |
| schema_save_mode            | 枚举   | 否       | CREATE_SCHEMA_WHEN_NOT_EXIST | Schema保存模式                                                                                        |
| data_save_mode              | 枚举   | 否       | APPEND_DATA                  | 数据保存模式                                                                                            |
| paimon.table.primary-keys   | 字符串 | 否       | -                            | 主键字段列表，联合主键使用逗号分隔(注意：分区字段需要包含在主键字段中)                                                              |
| paimon.table.partition-keys | 字符串 | 否       | -                            | 分区字段列表，多字段使用逗号分隔                                                                                  |
| paimon.table.write-props    | Map    | 否       | -                            | Paimon表初始化指定的属性, [参考](https://paimon.apache.org/docs/master/maintenance/configurations/#coreoptions) |
| paimon.hadoop.conf          | Map    | 否       | -                            | Hadoop配置文件属性信息                                                                                    |
| paimon.hadoop.conf-path     | 字符串 | 否       | -                            | Hadoop配置文件目录，用于加载'core-site.xml', 'hdfs-site.xml', 'hive-site.xml'文件配置                            |

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

### 单表(使用Hive catalog)

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        pk_id = bigint
        name = string
        score = int
      }
      primaryKey {
        name = "pk_id"
        columnNames = [pk_id]
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [1, "A", 100]
      },
      {
        kind = INSERT
        fields = [2, "B", 100]
      },
      {
        kind = INSERT
        fields = [3, "C", 100]
      },
      {
        kind = INSERT
        fields = [3, "C", 100]
      },
      {
        kind = INSERT
        fields = [3, "C", 100]
      },
      {
        kind = INSERT
        fields = [3, "C", 100]
      }
      {
        kind = UPDATE_BEFORE
        fields = [1, "A", 100]
      },
      {
        kind = UPDATE_AFTER
        fields = [1, "A_1", 100]
      },
      {
        kind = DELETE
        fields = [2, "B", 100]
      }
    ]
  }
}

sink {
  Paimon {
    schema_save_mode = "RECREATE_SCHEMA"
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

### 动态分桶paimon单表

只有在主键表并指定bucket = -1时才会生效

#### 核心参数：[参考官网](https://paimon.apache.org/docs/master/primary-key-table/data-distribution/#dynamic-bucket)

|               名称               |  类型  | 是否必须 |   默认值    |        描述        |
|--------------------------------|------|------|----------|------------------|
| dynamic-bucket.target-row-num  | long | 是    | 2000000L | 控制一个bucket的写入的行数 |
| dynamic-bucket.initial-buckets | int  | 否    |          | 控制初始化桶的数量        |

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
        bucket = -1
        dynamic-bucket.target-row-num = 50000
    }
    paimon.table.partition-keys = "dt"
    paimon.table.primary-keys = "pk_id,dt"
  }
}
```

### 多表

#### 示例1

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

#### 示例2

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = oracle.jdbc.driver.OracleDriver
    url = "jdbc:oracle:thin:@localhost:1521/XE"
    user = testUser
    password = testPassword

    table_list = [
      {
        table_path = "TESTSCHEMA.TABLE_1"
      },
      {
        table_path = "TESTSCHEMA.TABLE_2"
      }
    ]
  }
}

transform {
}

sink {
  Paimon {
    catalog_name="seatunnel_test"
    warehouse="file:///tmp/seatunnel/paimon/hadoop-sink/"
    database="${schema_name}_test"
    table="${table_name}_test"
  }
}
```
