import ChangeLog from '../changelog/connector-paimon.md';

# Paimon

> Paimon 源连接器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于从 `Apache Paimon` 读取数据

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

## 主要功能

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 配置选项

| 名称                      | 类型       | 是否必须   | 默认值 | 描述 |
|-------------------------|----------|--------|---------------|---|
| warehouse               | String   | 是      | -             | Paimon warehouse 路径，不能为空白字符串；`rest` 类型使用托管 warehouse 名称而不是路径。 |
| catalog_name            | String   | 否      | paimon        | Paimon Catalog 名称。 |
| catalog_type            | String   | 否      | filesystem    | Paimon Catalog 类型，支持 `filesystem`、`hive` 和 `rest`。 |
| catalog_uri             | String   | 当 `catalog_type` 为 `hive` 或 `rest` 时是 | -             | Paimon Catalog URI：Hive Metastore 地址或 REST Catalog 端点；`catalog_type` 为 `hive` 或 `rest` 时必须配置。 |
| token.provider          | String   | `catalog_type` 为 `rest` 时是 | -             | REST Catalog 的认证方式，只能是 `bear` 或 `dlf`。 |
| token                   | String   | `token.provider` 为 `bear` 时是 | -             | REST Catalog 的 Bearer token。 |
| dlf.access-key-id       | String   | `token.provider` 为 `dlf` 时是 | -             | DLF 的 Access Key ID。 |
| dlf.access-key-secret   | String   | `token.provider` 为 `dlf` 时是 | -             | DLF 的 Access Key Secret。 |
| dlf.security-token      | String   | 否 | -             | 可选的 DLF 临时安全令牌。 |
| dlf.region              | String   | 否 | -             | DLF 区域；未配置时从 `catalog_uri` 推断。 |
| database                | String   | 是      | -             | 需要访问的数据库。 |
| table                   | String   | 未配置 `table_list` 时是 | -             | 需要访问的表；必须在 `table` 和 `table_list` 中选择一个配置。 |
| table_list              | array    | 未配置 `table` 时是 | -             | 需要读取的表列表；每项必须包含 `database` 和 `table`，可单独配置 `query`。 |
| user                    | String   | 否      | -             | 访问 Paimon 表的用户名。 |
| password                | String   | 否      | -             | 访问 Paimon 表的密码，需要与 `user` 一起配置。 |
| hdfs_site_path          | String   | 否      | -             | 已弃用的 `hdfs-site.xml` 路径；建议使用 `paimon.hadoop.conf` 或 `paimon.hadoop.conf-path`。 |
| query                   | String   | 否      | -             | 读取表时使用的过滤查询；未配置时读取所有行。 |
| paimon.hadoop.conf      | Map      | 否      | -             | 应用到 Hadoop 配置的属性。 |
| paimon.hadoop.conf-path | String   | 否      | -             | `core-site.xml`、`hdfs-site.xml` 和 `hive-site.xml` 的加载路径。 |

### warehouse [string]

Paimon warehouse 路径；REST catalog 使用托管 warehouse 名称。不能为空白字符串。

### catalog_type [string]

Paimon Catalog 类型，支持 `filesystem`、`hive` 和 `rest`。

### catalog_uri [string]

Hive metastore URI 或 REST catalog 端点。当 `catalog_type` 为 `hive` 或 `rest` 时必须配置。

### database [string]

需要访问的数据库

### table [string]

需要访问的表。`table` 和 `table_list` 必须二选一配置。

### table_list [array]

`Paimon` 表名列表。当一个 Source 需要读取多张 Paimon 表时，使用此配置代替 `table`。`table` 和 `table_list` 必须二选一配置。每个配置项必须包含 `database` 和 `table`，也可以配置该表自己的 `query`。

### hdfs_site_path [string]

`hdfs-site.xml` 文件地址。该选项已废弃，新作业建议使用 `paimon.hadoop.conf` 或 `paimon.hadoop.conf-path`。

### query [string]

读取表格的筛选条件，例如：`select * from st_test where id > 100`。如果未指定，则将读取所有记录。 

目前，`where` 支持`<, <=, >, >=, =, !=, or, and,is null, is not null, between...and, in , not in, like`，其他暂不支持。 

Projection 已支持,你可以选择特定的列，例如：select id, name from st_test where id > 100。

由于 Paimon 限制，目前不支持 `Having`, `Group By` 和 `Order By`。

query 参数支持动态参数设置:
```sql
SELECT * FROM table /*+ OPTIONS('incremental-between' = 'test-tag1,test-tag2') */;
```


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
* time

### paimon.hadoop.conf [string]

hadoop conf 属性

### paimon.hadoop.conf-path [string]

指定 'core-site.xml', 'hdfs-site.xml', 'hive-site.xml' 文件加载路径。

## 文件系统

Paimon 连接器支持从多个文件系统读取数据。目前，支持的文件系统有 `hdfs` 和 `s3`。
如果使用 `s3` 文件系统，可以在 `paimon.hadoop.conf` 中配置`fs.s3a.access-key`、`fs.s3a.secret-key`、`fs.s3a.endpoint`、`fs.s3a.path.style.access`、`fs.s3a.aws.credentials.provider` 属性，数仓地址应该以 `s3a://` 开头。

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
source {
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
source {
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

### 读取多表

```hocon
source {
  Paimon {
    warehouse = "/tmp/paimon"
    database = "default"
    table_list = [
      {
        database = "default"
        table = "table1"
        query = "select * from table1 where id > 100"
      },
      {
        database = "default"
        table = "table2"
        query = "select * from table2 where id > 100"
      }
    ]
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
      hadoop_user_name = "hdfs"
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

### paimon开启权限示例

```hocon
source {
 Paimon {
     warehouse = "/tmp/paimon"
     database = "default"
     table = "st_test"
     user = "paimon"
     password = "******"
   }
}
```

## 读取 Paimon 表的 changelog

如果要读取 paimon 表的 changelog，首先要为 Paimon 源表设置 `changelog-producer`，然后使用 SeaTunnel 流任务读取。

### Note

目前，批读取总是读取最新的快照，如需读取更完整的 changelog 数据，需使用流读取，并在将数据写入 Paimon 表之前开始流读取，为了确保顺序，流读取任务并行度应该设置为 1。

### Streaming read 示例
```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
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

## FAQ

### Paimon 源连接器支持哪些读取模式？

SeaTunnel Paimon 源连接器支持批处理快照读取与流式 changelog 消费。在批处理模式下，可扫描指定或最新快照；在流式模式下，持续监听并拉取新提交的增量数据变更。

### 如何配置 Paimon 的存储路径与 Catalog？

通过 `warehouse` 配置底层存储根路径（如 `hdfs:///paimon/warehouse`、`s3a://bucket/warehouse` 或本地路径），并通过 `catalog_type` 指定 Catalog 类型（如 `filesystem`、`hive` 等）。相关的存储认证参数可统一在 `paimon.hadoop.conf` 中声明。

使用 `catalog_type = "rest"` 时，`warehouse` 应为托管仓库名称而非存储路径，并按上述示例配置 `catalog_uri` 和 Catalog 认证参数。底层存储参数仍通过 `paimon.hadoop.conf` 配置，与 REST Catalog 凭据分开。

### Paimon 源连接器是否支持列投影？

支持。Paimon 源连接器仅读取上游作业 Schema 中声明的目标字段，对底层 ORC 或 Parquet 等列式存储格式避免冗余数据反序列化，从而提升查询性能。

## 变更日志

<ChangeLog />
