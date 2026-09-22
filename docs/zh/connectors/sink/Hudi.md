import ChangeLog from '../changelog/connector-hudi.md';

# Hudi

> Hudi 接收器连接器

## 描述

用于将数据写入 Hudi。

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [x] [定时刷新](../../introduction/concepts/connector-v2-features.md)

:::tip 精确一次语义

精确一次是可选能力，需要设置 `semantics = EXACTLY_ONCE` 并开启 checkpoint。默认的 `AT_LEAST_ONCE`
语义下，作业重试可能把失败尝试的记录重复提交一次，详见[精确一次语义](#精确一次语义)。

:::

:::caution Hive Metastore 同步

SeaTunnel Hudi sink 会写入 Hudi 数据文件和 `.hoodie` 元数据，但不会在 Hive Metastore 中注册表或将表同步到 Hive Metastore。`hoodie.datasource.hive_sync.*` 配置不是受支持的 sink 选项，也不会传递给 Hudi 写入客户端。需要注册到 Hive Metastore 时，请单独运行 Apache Hudi `HiveSyncTool` 或其他表注册流程。

:::

## 选项

基础配置:

| 名称                       | 类型   | 是否必填 | 默认值                       |
|----------------------------|--------|----------|------------------------------|
| table_dfs_path             | string | 是      | -                            |
| conf_files_path            | string | 否      | -                            |
| table_list                 | array  | 否      | -                            |
| schema_save_mode           | enum   | 否      | CREATE_SCHEMA_WHEN_NOT_EXIST |
| data_save_mode             | enum   | 否      | APPEND_DATA                  |
| semantics                  | enum   | 否      | AT_LEAST_ONCE                |
| common-options             | config | 否      | -                            |

表清单配置:

| 名称                       | 类型    | 是否必填 | 默认值         |
|----------------------------|--------|----------|---------------|
| table_name                 | string | 是       | -             |
| database                   | string | 否       | default       |
| table_type                 | enum   | 否       | COPY_ON_WRITE |
| op_type                    | enum   | 否       | INSERT        |
| record_key_fields          | string | 否       | -             |
| partition_fields           | string | 否       | -             |
| precombine_field           | string | 否       | -             |
| batch_interval_ms          | int    | 否       | 1000          |
| batch_size                 | int    | 否       | 1000          |
| insert_shuffle_parallelism | int    | 否       | 2             |
| upsert_shuffle_parallelism | int    | 否       | 2             |
| min_commits_to_keep        | int    | 否       | 20            |
| max_commits_to_keep        | int    | 否       | 30            |
| index_type                 | enum   | 否       | BLOOM         |
| index_class_name           | string | 否       | -             |
| record_byte_size           | int    | 否       | 1024          |
| cdc_enabled                | boolean| 否       | false         |

注意：写入单表时，可以把 `table_list` 中的表配置项平铺到外层。

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

### index_type [string]

`index_type` Hudi 表的索引类型。当前支持 `BLOOM`、`SIMPLE`、`GLOBAL_BLOOM`。

### index_class_name [string]

`index_class_name` Hudi 表自定义索引名称，例如: `org.apache.seatunnel.connectors.seatunnel.hudi.index.CustomHudiIndex`.

### record_byte_size [Int]

`record_byte_size` Hudi 表单行记录的大小, 该值可用于预估每个hudi数据文件中记录的大致数量。调整此参数与`batch_size`可以有效减少hudi数据文件写放大次数.

### conf_files_path [string]

`conf_files_path` 环境配置文件路径列表（本地路径），用于初始化 HDFS 客户端以读取 Hudi 表文件。示例："/home/test/hdfs-site.xml;/home/test/core-site.xml;/home/test/yarn-site.xml"。

### op_type [enum]

`op_type` Hudi 表的操作类型。值可以是 `insert`、`upsert` 或 `bulk_insert`。

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

### semantics [Enum]

`semantics` 控制 Sink 以哪种语义把记录提交到 Hudi 表：

- `AT_LEAST_ONCE`：默认值。每个刷新批次由 Hudi 客户端立即提交，数据一刷新即可见，但作业重试时可能把失败尝试的记录再次提交。
- `EXACTLY_ONCE`：记录先写入一个 Hudi instant，只有包含它的 checkpoint 完成后才提交该 instant，从而提供精确一次语义。该语义要求开启 checkpoint，并且数据只在 checkpoint 完成后才可见，详见[精确一次语义](#精确一次语义)。

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

Hudi 定时刷新复用连接器现有的同步批量刷新。默认的 `AT_LEAST_ONCE` 语义下，Hudi 客户端会立即提交每个刷新批次，
因此定时刷新提供的是至少一次语义，重试可能产生额外的 commit。当 `semantics = EXACTLY_ONCE` 时，定时刷新只会把
记录写入当前 checkpoint 对应的 instant，这些记录会在该 checkpoint 完成后才可见。

## 精确一次语义

设置 `semantics = EXACTLY_ONCE` 后，Sink 会使用引擎的两阶段提交协议写入：

```hocon
env {
  checkpoint.interval = 10000
}

sink {
  Hudi {
    table_dfs_path = "/tmp/seatunnel_mnt/hudi"
    table_name = "st_test"
    semantics = "EXACTLY_ONCE"
    op_type = "UPSERT"
    record_key_fields = "c_bigint"
  }
}
```

工作流程：

1. 作业运行期间，写入器只把记录写入一个 Hudi instant，不会提交它；同一个 checkpoint 的所有批次共用同一个 instant。
2. checkpoint 完成后，聚合提交器（aggregated committer）才提交该 checkpoint 对应的 instant。提交之后文件才对读取方可见。
3. checkpoint 的提交信息会作为 checkpoint 状态保存。如果提交丢失（例如作业在 checkpoint 完成之后、提交之前失败），
   作业从该 checkpoint 恢复后会重新提交同一个 instant；已经提交过的 instant 会被跳过，因此记录不会重复发布。
4. 未完成的 checkpoint 的记录不会被提交：流水线重启后由 source 重放这些记录，失败尝试留下的 instant 会在写入器心跳
   过期后由 Hudi 自行回滚。

约束与取舍：

* 必须开启 checkpoint，否则不会提交任何 instant，数据也不会可见。没有 checkpoint 的批处理作业或需要“刷新即可见”的
  作业请使用 `AT_LEAST_ONCE`。
* 数据只在 checkpoint 完成后可见，因此可见性延迟至少是一个 checkpoint 间隔。
* instant 由聚合提交器在引擎完成 checkpoint（Zeta、Flink）或微批（Spark）后提交，因此需要使用会提交成功
  checkpoint 对应 commit info 的引擎。
* 并行写入仍然遵循 Hudi 的单写入者并发控制，两个作业（或两个写入器）更新同一个 file group 时可能互相覆盖。请保证
  同一张表只有一个写入者，或让写入器处理互不相交的 key 和分区。
* 如果提交时 instant 已经不在 active timeline 上（例如写入器心跳在提交前过期），作业会以明确的错误失败，而不是静默
  发布不完整的数据。

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

## 变更日志

<ChangeLog />
