import ChangeLog from '../changelog/connector-paimon.md';

# Paimon

> Paimon 数据连接器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

Apache Paimon数据连接器。支持cdc写以及自动建表。

### SeaTunnel与Paimon版本对照

| Seatunnel Version | Paimon Version   |
|-------------------|------------------|
| 2.3.2  -  2.3.3   | 0.4-SNAPSHOT     |
| 2.3.4             | 0.6-SNAPSHOT     |
| 2.3.5  -  2.3.11  | 0.7.0-incubating |
| 2.3.12  - 2.3.13  | 1.1.1            |

### 从 0.7 版本升级到 1.1.1 版本的注意事项

1. **备份建议**
   尽管存在兼容性保障，但在从 0.7 版本开始升级前，仍强烈建议备份关键数据，尤其是元数据目录。
2. **逐步升级流程**
   - **测试环境验证**：首先在测试环境中验证（从 0.7 版本开始的）升级过程。
   - **更新 JAR 文件**：将 Paimon 的 JAR 文件替换为 1.1.1 版本。
   - **自动格式升级**：系统会自动识别并升级 0.7 版本中使用的文件格式。
3. **配置检查**
   检查配置以确认是否存在 0.7 版本适用的已弃用选项。尽管大多数配置保持向后兼容，但已弃用的设置可能需要更新以适配 1.1.1 版本。
4. **升级后验证**
   从 0.7 版本升级到 1.1.1 版本后，需验证以下内容：
   - **读写操作**：确保基于 0.7 版本继承的数据结构，数据写入和读取流程正常运行。
   - **查询性能**：考虑到 0.7 与 1.1.1 版本间底层机制（如分桶管理）的变化，确认查询响应时间符合预期。
   - **新功能验证**：测试所有新增功能（如增强的压实机制、时间旅行等），确保其与从 0.7 版本迁移的数据兼容并正常工作。

**注意**：遵循这些步骤有助于降低风险，确保从 0.7 版本平稳过渡到稳定版本 1.1.1。

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

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 连接器选项

| 名称                           | 类型   | 是否必须 | 默认值                          | 描述                                                                                                   |
|------------------------------|------|------|------------------------------|------------------------------------------------------------------------------------------------------|
| warehouse                    | 字符串  | 是    | -                            | warehouse 路径；REST catalog 使用托管 warehouse 名称，而不是存储路径。 |
| catalog_name                 | 字符串  | 否    | paimon                       | Paimon catalog名称                                                                                     |
| catalog_type                 | 字符串  | 否    | filesystem                   | catalog 类型：`filesystem`、`hive` 或 `rest`。 |
| catalog_uri                  | 字符串  | `hive` 或 `rest` 时必填 | -                            | Hive metastore URI 或 REST catalog 服务端点。 |
| token.provider | String | `rest` 时必填 | - | 认证提供者：仅小写 `bear` 或 `dlf`。 |
| token | String | `bear` 时必填 | - | Bearer token。 |
| dlf.access-key-id | String | `dlf` 时必填 | - | DLF access key ID。 |
| dlf.access-key-secret | String | `dlf` 时必填 | - | DLF access key secret。 |
| dlf.security-token | String | 否 | - | 可选的临时 DLF security token。 |
| dlf.region | String | 否 | - | DLF region；未配置时从 `catalog_uri` 推断。 |
| database                     | 字符串  | 是    | -                            | 数据库名称，不能为空白字符串。                                                                                        |
| table                        | 字符串  | 是    | -                            | 表名，不能为空白字符串。                                                                                          |
| user                         | 字符串  | 否    | -                            | paimon开启权限后，用户名                                                                                      |
| password                     | 字符串  | 否    | -                            | paimon开启权限后，用户名对应密码                                                                                  |
| hdfs_site_path               | 字符串  | 否    | -                            | 已废弃。hdfs-site.xml 文件路径。新作业建议使用 `paimon.hadoop.conf` 或 `paimon.hadoop.conf-path`                         |
| schema_save_mode             | 枚举   | 否    | CREATE_SCHEMA_WHEN_NOT_EXIST | Schema保存模式                                                                                           |
| data_save_mode               | 枚举   | 否    | APPEND_DATA                  | 数据保存模式                                                                                               |
| paimon.table.primary-keys    | 字符串  | 否    | -                            | 主键字段列表，联合主键使用逗号分隔(注意：分区字段需要包含在主键字段中)                                                                 |
| paimon.table.partition-keys  | 字符串  | 否    | -                            | 分区字段列表，多字段使用逗号分隔                                                                                     |
| paimon.table.write-props     | Map  | 否    | -                            | Paimon表初始化指定的属性, [参考](https://paimon.apache.org/docs/master/maintenance/configurations/#coreoptions) |
| table_options                | Map  | 否    | -                            | Sink 侧表属性，仅用于 SaveMode 自动建表（不进入运行时 write-props）。详见下文。                                          |
| paimon.hadoop.conf           | Map  | 否    | -                            | Hadoop配置文件属性信息                                                                                       |
| paimon.hadoop.conf-path      | 字符串  | 否    | -                            | Hadoop配置文件目录，用于加载'core-site.xml', 'hdfs-site.xml', 'hive-site.xml'文件配置                               |
| paimon.table.non-primary-key | Boolean | 否    | false                        | 控制创建主键表或者非主键表. 当为true时,创建非主键表, 为false时,创建主键表                                                         |
| branch                       | 字符串  | 否    | -                            | 要写入数据的 Paimon 表分支名称。不配置时写入 main 分支。非 main 分支要求 main 表和目标分支已存在，且不支持 `schema_save_mode=RECREATE_SCHEMA` 或 `data_save_mode=DROP_DATA`。 |
| multi_table_sink_replica     | int    | 否    | 1                            | 多表写入模式下每张表的 writer 副本数。当一个上游作业写入多张 Paimon 表、且每张表需要多个 sink writer 时使用。                                      |

### table_options [Map]

Sink 侧表属性，仅在 SaveMode 自动建表时生效（例如 `schema_save_mode` 为 `CREATE_SCHEMA_WHEN_NOT_EXIST` 或 `RECREATE_SCHEMA`）。**不会**对已存在表执行 ALTER，也**不会**改变由 `paimon.table.write-props` 派生的运行时 writer 设置（例如 `changelog-producer` / `changelog-tmp-path`）。

`table_options` 会合并进自动建表使用的 schema options。**键冲突时以 `paimon.table.write-props` 为准**，保证仅配置 write-props 的存量作业行为不变。`paimon.table.write-props` 仍作为运行时 writer 配置。键名使用 [Paimon CoreOptions](https://paimon.apache.org/docs/master/maintenance/configurations/#coreoptions)；SeaTunnel 不维护白名单，非法键在 Paimon 建表时失败。空白键与 null 值会在作业提交阶段被拒绝。

示例：

```hocon
sink {
  Paimon {
    warehouse = "file:///tmp/paimon"
    database = "seatunnel"
    table = "test"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    table_options = {
      bucket = "4"
      file.format = "parquet"
    }
    # 同名键时覆盖 table_options
    paimon.table.write-props = {
      bucket = "8"
    }
  }
}
```

## 批模式下的checkpoint

当您在批处理模式下将`checkpoint.interval`设置为大于0的值时，在写入一定数量的记录后checkpoint触发时，paimon连接器将把数据提交到paimon表。此时，写入的数据是可见的。
但是，如果您没有在批处理模式下设置`checkpoint.interval`，则在写入所有记录之后，paimon sink连接器将提交数据。到批任务完成之前，写入的数据都是不可见的。

## Changelog 生产者
配置 `changelog-producer` 以启用 Paimon 表的 changelog 生产者模式。如果你使用了 Paimon sink 的自动建表功能，请在 `paimon.table.write-props` 或 `table_options` 中设置该属性；对于在 SeaTunnel 之外创建的已存在表，请直接在该表上设置。

Paimon表的changelog产生模式有[四种](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/)，分别是`none`、`input`、`lookup` 和 `full-compaction`。

目前支持全部`changelog-producer`模式。默认是`none`模式。

* [`none`](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/#none)
* [`input`](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/#input)
* [`lookup`](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/#lookup)
* [`full-compaction`](https://paimon.apache.org/docs/master/primary-key-table/changelog-producer/#full-compaction)
> 注意：
> 当你使用流模式去读paimon表的数据时，不同模式将会产生[不同的结果](../source/Paimon.md#读取-paimon-表的-changelog)。

## 文件系统
Paimon连接器支持向多文件系统写入数据。目前支持的文件系统有hdfs和s3。
如果您使用s3文件系统。您可以配置`fs.s3a.access-key `， `fs.s3a.secret-key`， `fs.s3a.endpoint`， `fs.s3a.path.style.access`， `fs.s3a.aws.credentials.provider`。在`paimon.hadoop.conf`选项中设置提供程序的属性。
除此之外，warehouse应该以`s3a://`开头。

## 模式演变
Cdc采集支持有限数量的模式更改。目前支持的模式更改包括：

* 添加列。

* 修改列。更具体地说，如果修改列类型，则支持以下更改：

    * 将字符串类型（char、varchar、text）更改为另一种长度更长的字符串类型，
    * 将二进制类型（binary, varbinary, blob）更改为另一种长度更长的二进制类型，
    * 将整数类型（tinyint, smallint, int, bigint）更改为另一种范围更大的整数类型，
    * 将浮点类型（float、double）更改为另一种范围更大的浮点类型，

> 注意:
> 
> 如果{oldType}和{newType}属于同一个类型族，但旧类型的精度高于新类型。忽略这个转换。

* 删除列。

* 重命名列（可与上文“修改列类型”中列出的类型加宽、列位置、注释、可空性变更同时进行——当新旧列名不同时，变更处理器会在同一步骤中应用这些更新并重命名该列）。

## 示例

### REST catalog 认证

`catalog_type = "rest"` 使用 Paimon 1.1.1 内置的 REST 实现。`catalog_uri` 为 HTTPS catalog 端点，`warehouse` 为托管 warehouse **名称**，而不是 HDFS/S3 路径。表的存储位置由 REST 服务返回；worker 仍需具有底层存储的访问权限和凭证。现有 filesystem 和 Hive 配置保持不变。

REST 的 `catalog_uri` 必须是包含主机且不含用户信息的 HTTP 或 HTTPS URL。除回环地址测试外，请使用 HTTPS：非回环主机使用 HTTP 会产生警告，因为认证凭证会明文传输。`token.provider` 必填，且仅接受小写 `bear` 或 `dlf`。`bear` 要求非空白的 `token`；`dlf` 要求同时配置 `dlf.access-key-id` 和 `dlf.access-key-secret`。

Bearer token 和 DLF 凭证在解析后的配置日志中自动脱敏。配置加密时，`token` 默认包含在 `shade.identifier` 的处理范围内；DLF 密钥必须显式加入 `shade.options` 才能加密和解密。日志脱敏本身不会加密配置。不要在端点 URL 中放置凭证。请通过环境变量或部署环境的密钥机制传入。

REST 仅转发显式配置的 Hadoop 存储参数。`paimon.hadoop.conf` 和指定的 Hadoop XML 文件中禁止配置 catalog 和认证键（`uri`、`warehouse`、`metastore`、`token`、`token.*`、`dlf.*`，包括带 `hadoop.` 前缀的形式）。请通过下方的顶层选项配置 REST 认证。此版本不支持 `dlf.token-path`、`dlf.token-loader`、`dlf.token-ecs-*` 和 `token.refresh-time`，也不能通过 `paimon.hadoop.conf` 启用它们。

解析后的配置脱敏不会过滤依赖库日志。Paimon 1.1.1 在 `org.apache.paimon.utils.HadoopUtils` 启用 DEBUG 日志时会记录 Hadoop 存储参数的值，其中可能包含凭据。使用包含凭据的存储配置时，请将此日志级别保持为 INFO 或更高。

Paimon 1.1.1 的共享 OkHttp 客户端会跟随跨主机重定向并转发 `x-dlf-security-token`，但会移除 bearer `Authorization`。请使用可信且不会重定向到其他主机的 catalog 端点，不要依赖重定向保护 DLF 临时凭证。此连接器没有修改该依赖库限制。

Bearer 示例：

```hocon
sink {
  Paimon {
    catalog_type = "rest"
    catalog_uri = "https://catalog.example.com"
    warehouse = "managed-warehouse"
    database = "example_db"
    table = "example_table"
    "token.provider" = "bear"
    token = ${PAIMON_REST_TOKEN}
  }
}
```

DLF 示例（将 `catalog_uri` 替换为实际 DLF 端点）：

```hocon
sink {
  Paimon {
    catalog_type = "rest"
    catalog_uri = "https://catalog.example.com"
    warehouse = "managed-warehouse"
    database = "example_db"
    table = "example_table"
    "token.provider" = "dlf"
    "dlf.access-key-id" = ${DLF_ACCESS_KEY_ID}
    "dlf.access-key-secret" = ${DLF_ACCESS_KEY_SECRET}
    "dlf.security-token" = ${?DLF_SECURITY_TOKEN}
    "dlf.region" = "cn-hangzhou"
  }
}
```

`dlf.security-token` 是可选参数，仅临时凭证需要。若 Paimon 可从 DLF 端点推断 region，则可省略 `dlf.region`。此实现传递静态凭证，不提供临时凭证自动刷新。请在目标部署环境中验证真实 DLF 认证及云存储权限。


### 模式演变
```hocon
env {
  # You can set engine configuration here
  parallelism = 5
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  read_limit.bytes_per_second=7000000
  read_limit.rows_per_second=400
}

source {
  MySQL-CDC {
    server-id = 5652-5657
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    
    schema-changes.enabled = true
  }
}

sink {
  Paimon {
    warehouse = "file:///tmp/paimon"
    database = "mysql_to_paimon"
    table = "products"
  }
}
```

### 单表

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
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

### 单表(基于S3文件系统)

```hocon
env {
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_bytes = bytes
        c_date = date
        c_decimal = "decimal(38, 18)"
        c_timestamp = timestamp
      }
    }
  }
}

sink {
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
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
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

### 单表(指定hadoop HA配置和指定hadoop用户名)

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
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
      hadoop_user_name = "hdfs"
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
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
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
#### 使用`changelog-producer`属性写入

```hocon
env {
 parallelism = 1
 job.mode = "STREAMING"
 checkpoint.interval = 5000
}

source {
 Mysql-CDC {
  url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
  username = "root"
  password = "******"
  table-names = ["seatunnel.role"]
 }
}

sink {
 Paimon {
  catalog_name = "seatunnel_test"
  warehouse = "file:///tmp/seatunnel/paimon/hadoop-sink/"
  database = "seatunnel"
  table = "role"
  paimon.table.write-props = {
   changelog-producer = full-compaction
   changelog-tmp-path = /tmp/paimon/changelog
  }
 }
}
```

### 动态分桶paimon单表

只有在主键表并指定bucket = -1时才会生效

> 注意: 
> - 目前只支持普通动态桶模式(主键包含所以分区字段)。
> - 在集群环境下运行时`parallelism`必须为`1`, 否则可能存在数据重复问题。

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
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
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
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
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

### paimon开启权限认证

#### 示例1

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    table-names = ["seatunnel.role","seatunnel.user","galileo.Bucket"]
  }
}

transform {
}

sink {
  Paimon {
    catalog_name = "seatunnel_test"
    warehouse = "file:///tmp/seatunnel/paimon/hadoop-sink/"
    database = "${database_name}"
    table = "${table_name}"
    user = "paimon"
    password = "******"
  }
}
```

## FAQ

### Paimon Sink 连接器是否支持自动建表？

支持。当配置 `schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"` 时，SeaTunnel 会自动根据上游传递的元数据信息初始化目标 Paimon 表（包含主键与分区配置）。

### Paimon Sink 如何保证精确一次（Exactly-Once）写入？

Paimon Sink 深度集成了 SeaTunnel Zeta、Flink 以及 Spark 的两阶段提交（2PC）检查点机制。在一个 Checkpoint 周期内写入的数据，会在 Checkpoint 屏障对齐并确认成功后作为快照正式提交。

### Paimon Sink 支持哪些表模式（主键表与追加表）？

两种表模式均受支持。若目标表配置了主键（`paimon.table.primary-keys`），Sink 将按主键执行 Upsert/Delete 逻辑；若未声明主键，则以 Append-Only 追加模式写入。

## 变更日志

<ChangeLog />
