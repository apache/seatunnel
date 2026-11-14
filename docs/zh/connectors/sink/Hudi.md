import ChangeLog from '../changelog/connector-hudi.md';

# Hudi

> Hudi 接收器连接器

## 描述

Hudi Sink 连接器把 SeaTunnel 的行写入到 Apache Hudi 表中，表可以放在 HDFS 或兼容 S3 的文件系统上。
它既支持单表作业，也支持多表作业，并提供 CDC 变更日志持久化、可配置的 commit 清理策略以及
可插拔的索引。

当需要把 SeaTunnel 的 CDC 输入（例如 MySQL-CDC、PostgreSQL-CDC）或批处理源落到 copy-on-write 或
merge-on-read 的 Hudi 表时，可以使用该连接器。它会写入 Hudi 数据文件以及 `.hoodie` 元数据，并允许通过
`op_type` 在 `INSERT`、`UPSERT`、`BULK_INSERT` 三种写入模式之间选择。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [x] [定时刷新](../../introduction/concepts/connector-v2-features.md)

:::caution Hive Metastore 同步

SeaTunnel Hudi sink 会写入 Hudi 数据文件和 `.hoodie` 元数据，但不会在 Hive Metastore 中注册表或将表同步到 Hive Metastore。`hoodie.datasource.hive_sync.*` 配置不是受支持的 sink 选项，也不会传递给 Hudi 写入客户端。需要注册到 Hive Metastore 时，请单独运行 Apache Hudi `HiveSyncTool` 或其他表注册流程。

:::

## 选项

基础配置:

| 名称                       | 类型   | 是否必填 | 默认值                       | 描述                                                                            |
|----------------------------|--------|----------|------------------------------|---------------------------------------------------------------------------------|
| table_dfs_path             | string | 是        | -                            | Hudi 表数据和元数据的根路径。 |
| conf_files_path            | string | 否        | -                            | HDFS 客户端配置文件的本地路径，多个路径用分号分隔。 |
| kerberos_principal         | string | 否        | -                            | Kerberos 身份，须与 kerberos_keytab_path 一起设置。 |
| kerberos_keytab_path       | string | 否        | -                            | catalog 和各 writer 进程可读取的本地 keytab 路径。 |
| krb5_path                  | string | 否        | -                            | 可选的本地 Kerberos 配置文件，会修改 JVM 全局配置。 |
| table_list                 | array  | 否        | -                            | 多表作业中每张表的配置。 |
| schema_save_mode           | enum   | 否        | CREATE_SCHEMA_WHEN_NOT_EXIST | 作业启动前如何处理目标 schema。 |
| data_save_mode             | enum   | 否        | APPEND_DATA                  | 作业启动前如何处理已存在的表数据。 |
| multi_table_sink_replica   | int    | 否        | 1                            | 多表写入模式下每张表写入器的副本数。 |
| common-options             | config | 否        | -                            | [Sink 插件通用参数](../common-options/sink-common-options.md)。 |

表清单配置:

| 名称                       | 类型    | 是否必填 | 默认值         | 描述                                                                            |
|----------------------------|---------|----------|----------------|----------------------------------------------------------------------------------|
| table_name                 | string  | 是        | -              | 目标 Hudi 表名。 |
| database                   | string  | 否        | default        | 目标 Hudi 数据库名。 |
| table_type                 | enum    | 否        | COPY_ON_WRITE  | Hudi 表类型：`COPY_ON_WRITE` 或 `MERGE_ON_READ`。 |
| op_type                    | enum    | 否        | INSERT         | 写入操作类型：`INSERT`、`UPSERT` 或 `BULK_INSERT`。 |
| record_key_fields          | string  | 否        | -              | 用于构建 Hudi record key 的字段；`UPSERT` 时必填。 |
| partition_fields           | string  | 否        | -              | 用于构建分区路径的字段。 |
| precombine_field           | string  | 否        | -              | 用于合并同一记录多次更新的字段。 |
| batch_interval_ms          | int     | 否        | 1000           | 当前未使用；刷写仅由 `batch_size` 或 checkpoint 触发。 |
| batch_size                 | int     | 否        | 1000           | 刷写前最多缓存的行数。 |
| insert_shuffle_parallelism | int     | 否        | 2              | insert 操作的 shuffle 并行度。 |
| upsert_shuffle_parallelism | int     | 否        | 2              | upsert 操作的 shuffle 并行度。 |
| min_commits_to_keep        | int     | 否        | 20             | 清理期间保留的最小提交数。 |
| max_commits_to_keep        | int     | 否        | 30             | 清理期间保留的最大提交数。 |
| index_type                 | enum    | 否        | BLOOM          | Hudi 索引类型：`BLOOM`、`SIMPLE` 或 `GLOBAL_BLOOM`。 |
| index_class_name           | string  | 否        | -              | 自定义 Hudi 索引类的全限定名。 |
| record_byte_size           | int     | 否        | 1024           | 以字节为单位的平均记录大小估计值。 |
| cdc_enabled                | boolean | 否        | false          | 启用后持久化 Hudi CDC 变更日志数据。 |

注意：写入单表时，可以把 `table_list` 中的表配置项平铺到外层。多表作业中，表级配置需放在各自的 `table_list` 条目内；`table_dfs_path`、`conf_files_path`、`schema_save_mode` 和 `data_save_mode` 保持在 sink 层级。

`record_key_fields` 在 `UPSERT` 模式下必填（启动时校验），在 `BULK_INSERT` 模式下也必填（当前未校验——缺少该配置会在写入时抛出 `NullPointerException`，而不是配置期错误）。对于 CDC 输入，上游记录必须包含 `record_key_fields` 引用的字段；仅当需要 Hudi CDC 变更日志时才设置 `cdc_enabled = true`。

### Kerberos 认证

在 sink 层同时设置非空的 `kerberos_principal` 和 `kerberos_keytab_path`，
多表作业也使用此层级。两者均未设置时保持原有认证行为；只设置其中一项或设置空白值会报错。
JDBC 风格的 `use.kerberos`、`kerberos.principal`、`kerberos.principal.file`
不是 Hudi 配置项。

`conf_files_path` 加载的配置必须包含 `hadoop.security.authentication=kerberos`
以及目标 HDFS 配置。加载 XML 本身不会登录；连接器会通过 keytab 登录，无需在 worker
启动前额外执行 keytab 登录。执行保存模式/catalog 初始化的进程及每个 writer worker
都必须能读取 keytab 和 XML 文件。不要将 keytab 内容写入作业配置。

```hocon
kerberos_principal = "seatunnel@EXAMPLE.COM"
kerberos_keytab_path = "/etc/security/keytabs/seatunnel.keytab"
krb5_path = "/etc/krb5.conf"
conf_files_path = "/etc/hadoop/core-site.xml;/etc/hadoop/hdfs-site.xml"
```

每个 catalog 和普通/缓存写入客户端分别持有登录返回的 UGI，文件系统及客户端初始化、
写入、删除、自动提交和清理均使用该身份，操作前检查票据续期。认证后的 Java 引擎并行流
使用隔离的身份线程池，避免进入已初始化的公共线程池后丢失身份。连接器不会替换 Hadoop
全局登录用户。Hadoop 安全模式初始化和 `java.security.krb5.conf` 仍是 JVM 全局配置，
共享 worker 的作业应使用兼容的 Hadoop 安全及 realm 配置。

无凭据测试覆盖身份传递和错误/清理路径，不代表已验证真实 KDC 登录或票据过期。
部署验证时，应在未预登录的进程中对开启 Kerberos 的 HDFS 分别执行 INSERT、
UPSERT/删除 CDC 作业，再覆盖多表模式、保存模式的 catalog 建表和作业关闭。
长时间运行流作业以验证票据续期，并确认并发的另一身份不受影响；还应检查 keytab
缺失/不可读及 KDC 不可达时的失败行为，确保日志不泄露凭据。

### table_name [string]

`table_name` Hudi 表的名称。

### database [string]

`database` Hudi 表所属的数据库。

### table_dfs_path [string]

`table_dfs_path` Hudi 表的 DFS 根路径，例如 "hdfs://nameservice/data/hudi/"。

### table_type [enum]

`table_type` Hudi 表的类型，可选值为 `COPY_ON_WRITE` 和 `MERGE_ON_READ`。

### record_key_fields [string]

`record_key_fields` Hudi 表的记录键字段。当 `op_type` 为 `UPSERT` 时，必须配置该项。

### partition_fields [string]

`partition_fields` Hudi 表的分区字段.

### precombine_field [string]

`precombine_field` Hudi 表的预合并字段,它用于在写入前进行预合并.

### index_type [enum]

`index_type` Hudi 表的索引类型。当前支持 `BLOOM`、`SIMPLE`、`GLOBAL_BLOOM`。

### index_class_name [string]

`index_class_name` Hudi 表自定义索引名称，例如: `org.apache.seatunnel.connectors.seatunnel.hudi.index.CustomHudiIndex`.

### record_byte_size [Int]

`record_byte_size` Hudi 表单行记录的大小, 该值可用于预估每个hudi数据文件中记录的大致数量。调整此参数与`batch_size`可以有效减少hudi数据文件写放大次数.

### conf_files_path [string]

`conf_files_path` 环境配置文件路径列表（本地路径），用于初始化 HDFS 客户端以读取 Hudi 表文件。示例："/home/test/hdfs-site.xml;/home/test/core-site.xml;/home/test/yarn-site.xml"。

### op_type [enum]

`op_type` Hudi 表的操作类型。值可以是 `INSERT`、`UPSERT` 或 `BULK_INSERT`。

### batch_interval_ms [Int]

`batch_interval_ms` 为兼容性保留。在 Zeta 上需要定时刷新时，请在作业 `env` 中配置
`sink.flush.interval`。

### batch_size [Int]

`batch_size` 单次刷新到 Hudi 前最多缓存的记录数。

### insert_shuffle_parallelism [Int]

`insert_shuffle_parallelism` 插入数据到 Hudi 表的并行度。

### upsert_shuffle_parallelism [Int]

`upsert_shuffle_parallelism` 更新插入数据到 Hudi 表的并行度。

### min_commits_to_keep [Int]

`min_commits_to_keep` Hudi 表保留的最少提交数。

### max_commits_to_keep [Int]

`max_commits_to_keep` Hudi 表保留的最多提交数。

### cdc_enabled [boolean]

`cdc_enabled` 是否持久化Hudi表的CDC变更日志。启用后，在必要时持久化更改数据，表可以作为CDC模式进行查询.

### schema_save_mode [Enum]

在启动同步任务之前，针对目标侧已有的表结构选择不同的处理方案<br/>
选项介绍：<br/>
`RECREATE_SCHEMA`：当表不存在时会创建，当表已存在时会删除并重建<br/>
`CREATE_SCHEMA_WHEN_NOT_EXIST`：当表不存在时会创建，当表已存在时则跳过创建<br/>
`ERROR_WHEN_SCHEMA_NOT_EXIST`：当表不存在时将抛出错误<br/>
`IGNORE` ：忽略对表的处理<br/>

### data_save_mode [Enum]

在启动同步任务之前，针对目标端已有数据选择不同的处理方案：<br/>
`DROP_DATA`：保留表结构并删除已有数据<br/>
`APPEND_DATA`：保留表结构和已有数据<br/>
`ERROR_WHEN_DATA_EXISTS`：当已有数据存在时报错<br/>

### 通用选项

Sink插件通用参数，请参考 [Sink Common Options](../common-options/sink-common-options.md) 了解详细信息。

## 定时刷新

定时刷新是仅由 Zeta 支持的引擎级能力。在作业的 `env` 中配置 `sink.flush.interval` 后，即使尚未达到
`batch_size`，Hudi Sink 也会写出待处理的记录。Spark 和 Flink 不会注入 `FlushSignal`，因此不会触发这种
定时刷新。

```hocon
env {
  sink.flush.interval = 5000
}
```

Hudi 定时刷新复用连接器现有的同步批量刷新和 Hudi 客户端 auto-commit 行为。Hudi Sink 没有 2PC 精确一次
写入器，因此定时刷新提供的是至少一次语义，重试可能产生额外的 commit。使用 `INSERT` 时，自动生成的
record key 还可能在恢复后产生重复行；使用具有稳定 `record_key_fields` 的 `UPSERT` 可以减少逻辑记录重复。

## 示例

### 单表 UPSERT

当 `op_type` 为 `UPSERT` 时，必须配置 `record_key_fields`。

```hocon
sink {
  Hudi {
    table_dfs_path = "/tmp/seatunnel_mnt/hudi"
    database = "st"
    table_name = "st_test"
    table_type = "COPY_ON_WRITE"
    op_type = "UPSERT"
    record_key_fields = "c_bigint"
    batch_size = 1000
    batch_interval_ms = 1000
  }
}
```

### 最小单表配置

追加写入时，通常只需要配置 `table_dfs_path` 和 `table_name`。

```hocon
sink {
  Hudi {
    table_dfs_path = "/tmp/seatunnel_mnt/hudi"
    table_name = "st_test"
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
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    
    table-names = ["seatunnel.role","seatunnel.user","galileo.Bucket"]
  }
}

transform {
}

sink {
  Hudi {
    table_dfs_path = "hdfs://nameserivce/data/"
    conf_files_path = "/home/test/hdfs-site.xml;/home/test/core-site.xml;/home/test/yarn-site.xml"
    table_list = [
      {
        database = "st1"
        table_name = "role"
        table_type = "COPY_ON_WRITE"
        op_type = "INSERT"
        batch_size = 10000
      },
      {
        database = "st1"
        table_name = "user"
        table_type = "COPY_ON_WRITE"
        op_type = "UPSERT"
        record_key_fields = "user_id"
        batch_size = 10000
      },
      {
        database = "st1"
        table_name = "Bucket"
        table_type = "MERGE_ON_READ"
      }
    ]
  }
}
```

### CDC 写入 Hudi

当 Hudi 表需要保存 CDC 变更日志信息时，可以开启 `cdc_enabled`。

```hocon
sink {
  Hudi {
    table_dfs_path = "/tmp/seatunnel_mnt/hudi"
    database = "st"
    table_name = "st_test"
    table_type = "COPY_ON_WRITE"
    op_type = "UPSERT"
    record_key_fields = "id"
    cdc_enabled = true
  }
}
```

### S3 存储

sink 可以写入 S3 兼容路径。`connector-hudi` 模块不依赖 `hadoop-aws`/`aws-java-sdk`，因此要解析 `s3a://` 协议，需要先将 `hadoop-aws` 和匹配的 AWS SDK 包（或 SeaTunnel 的 `seatunnel-hadoop-aws` jar）放入 `$SEATUNNEL_HOME/lib`（或连接器的插件 lib 目录），下面的示例才能运行。之后通过 `conf_files_path`（或运行时 classpath）提供所需的 Hadoop 文件系统配置，再使用 `s3a://` 表路径。

```hocon
sink {
  Hudi {
    table_dfs_path = "s3a://hudi/"
    conf_files_path = "/etc/hadoop/core-site.xml;/etc/hadoop/hdfs-site.xml"
    table_name = "st_test"
    op_type = "UPSERT"
    record_key_fields = "id"
  }
}
```

## 变更日志

<ChangeLog />
