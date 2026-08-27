import ChangeLog from '../changelog/connector-starrocks.md';

# StarRocks

> StarRocks 数据接收器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [CDC](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 描述

该接收器用于将数据写入到StarRocks中。支持批和流两种模式。
StarRocks数据接收器内部实现采用了缓存，通过stream load将数据批导入。

## 依赖

### 对于 Spark/Flink

> 1. 你需要下载 [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 并添加到目录 `${SEATUNNEL_HOME}/plugins/`.

### 对于 SeaTunnel Zeta

> 1. 你需要下载 [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 并添加到目录 `${SEATUNNEL_HOME}/lib/`.

## 接收器选项

|             名称              |   类型    | 是否必须 |             默认值              | 说明                                                                                                                  |
|-----------------------------|---------|------|------------------------------|---------------------------------------------------------------------------------------------------------------------|
| nodeUrls                    | list    | 是    | -                            | `StarRocks` 集群地址，格式为 `["fe_ip:fe_http_port", ...]`                                                                 |
| base-url                    | string  | 是    | -                            | JDBC URL 样式的连接信息。如：`jdbc:mysql://localhost:9030/`、`jdbc:mysql://localhost:9030` 或 `jdbc:mysql://localhost:9030/db` |
| username                    | string  | 是    | -                            | 目标 `StarRocks` 用户名                                                                                                  |
| password                    | string  | 是    | -                            | 目标 `StarRocks` 密码                                                                                                   |
| database                    | string  | 是    | -                            | 目标 StarRocks 表所在的数据库名称                                                                                             |
| table                       | string  | 否    | -                            | 目标 StarRocks 表名。如果没有设置，则表名与上游表名相同                                                                                 |
| labelPrefix                 | string  | 否    | -                            | StarRocks Stream Load 作业标签前缀                                                                                        |
| batch_max_rows              | long    | 否    | 1024                         | 批量写入时，当缓存行数达到 `batch_max_rows`、字节数达到 `batch_max_bytes`，或时间达到 `checkpoint.interval` 时，数据会刷新到 StarRocks        |
| batch_max_bytes             | int     | 否    | 5 * 1024 * 1024              | 批量写入时，当缓存行数达到 `batch_max_rows`、字节数达到 `batch_max_bytes`，或时间达到 `checkpoint.interval` 时，数据会刷新到 StarRocks        |
| max_retries                 | int     | 否    | -                            | 数据写入 StarRocks 失败后的重试次数                                                                                           |
| retry_backoff_multiplier_ms | int     | 否    | -                            | 用作生成下一次退避延迟的乘数                                                                                                      |
| max_retry_backoff_ms        | int     | 否    | -                            | 向 StarRocks 发送重试请求前的等待时长                                                                                            |
| enable_upsert_delete        | boolean | 否    | false                        | 是否开启 upsert/delete 事件同步，仅支持主键模型表                                                                                   |
| save_mode_create_template   | string  | 否    | 参见表下方的说明                     | 自动建表模板，详见表下方说明                                                                                                      |
| starrocks.config            | map     | 否    | -                            | Stream Load `data_desc` 参数                                                                                           |
| http_socket_timeout_ms      | int     | 否    | 180000                       | HTTP socket 超时时间，默认为 3 分钟                                                                                           |
| schema_save_mode            | Enum    | 否    | CREATE_SCHEMA_WHEN_NOT_EXIST | 同步任务启动前，针对目标端已存在的表结构选择不同处理方式                                                                                       |
| data_save_mode              | Enum    | 否    | APPEND_DATA                  | 同步任务启动前，针对目标端已存在的数据选择不同处理方式                                                                                         |
| custom_sql                  | String  | 否    | -                            | 当 `data_save_mode` 设置为 `CUSTOM_PROCESSING` 时必须配置。该 SQL 会在同步任务启动前执行                                                |

### save_mode_create_template

StarRocks数据接收器使用模板，在需求需要的时候也可以修改模板，并结合上游数据类型和结构生成表的创建语句来自动创建StarRocks表。当前仅在多表模式下有效。

默认模板如下：

```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table_name}` (
${rowtype_primary_key},
${rowtype_fields}
) ENGINE=OLAP
PRIMARY KEY (${rowtype_primary_key})
COMMENT '${comment}'
DISTRIBUTED BY HASH (${rowtype_primary_key})PROPERTIES (
"replication_num" = "1"
)
```

在模板中添加自定义字段，比如说加上`id`字段的修改模板如下：

```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table_name}`
(   
    id,
    ${rowtype_fields}
) ENGINE = OLAP 
    COMMENT '${comment}'
    DISTRIBUTED BY HASH (${rowtype_primary_key})
    PROPERTIES
(
    "replication_num" = "1"
);
```

StarRocks数据接收器根据上游数据自动获取相应的信息来填充模板，并且会移除`rowtype_fields`中的id字段信息。使用此方法可用来为自定义字段修改类型及相关属性。

可以使用的占位符有：

- database: 上游数据模式的库名称
- table_name: 上游数据模式的表名称
- rowtype_fields: 上游数据模式的所有字段信息，连接器会将字段信息自动映射到StarRocks对应的类型
- rowtype_primary_key: 上游数据模式的主键信息，结果可能是列表
- rowtype_unique_key: 上游数据模式的唯一键信息，结果可能是列表
- comment: 上游数据模式的注释信息

### table [string]

使用选项参数`database`和`table-name`自动生成SQL，并接收上游输入数据写入StarRocks中。

此选项与 `query` 是互斥的，具具有更高的优先级。

table选项参数可以填入一任意表名，这个名字最终会被用作目标表的表名，并且支持变量（`${table_name}`，`${schema_name}`）。
替换规则如下：`${schema_name}` 将替换传递给目标端的 SCHEMA 名称，`${table_name}` 将替换传递给目标端的表名。

例如：
1. test_${schema_name}_${table_name}_test
2. sink_sinktable
3. ss_${table_name}

### schema_save_mode [Enum]

在同步任务打开之前，针对目标端已存在的表结构选择不同的处理方法。可选值有：  
`RECREATE_SCHEMA` ：不存在的表会直接创建，已存在的表会删除并根据参数重新创建  
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：忽略已存在的表，不存在的表会直接创建  
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：当有不存在的表时会直接报错  
`IGNORE` ：忽略对表的处理

### data_save_mode [Enum]

在同步任务打开之前，针对目标端已存在的数据选择不同的处理方法。可选值有：
`DROP_DATA`： 保存数据库结构，但是会删除表中存量数据
`APPEND_DATA`：保存数据库结构和相关的表存量数据
`CUSTOM_PROCESSING`：自定义处理
`ERROR_WHEN_DATA_EXISTS`：当对应表存在数据时直接报错

### custom_sql [String]

当data_save_mode设置为CUSTOM_PROCESSING时，必须同时设置CUSTOM_SQL参数。CUSTOM_SQL的值为可执行的SQL语句，在同步任务开启前SQL将会被执行。

### table_options [Map]

Sink 在 SaveMode 自动建表（DDL）时附加的表级属性。仅在 `schema_save_mode` 触发建表时生效，例如 `CREATE_SCHEMA_WHEN_NOT_EXIST`、`RECREATE_SCHEMA`；**不影响** Stream Load 写入，也**不会**对已存在表执行 `ALTER TABLE`。

在默认 `save_mode_create_template`（未配置或与内置默认值相同）下，`table_options` 会合并进模板 `PROPERTIES` 子句；**同名 key 以 `table_options` 为准**。属性名请参考 [StarRocks CREATE TABLE 文档](https://docs.starrocks.io/docs/sql-reference/sql-statements/table_bucket_part_index/CREATE_TABLE/#properties)；SeaTunnel 不做白名单，非法属性由 StarRocks 执行 DDL 时报错。

若配置了**与内置默认值不同**的 `save_mode_create_template`，则不能与 `table_options` 同时使用（任务提交时校验失败）；此时请将属性直接写入模板。

非法组合会在 `StarRocksSinkFactory` 的 option 规则阶段提前校验（`--check` 与作业提交），而非仅在 StarRocks 执行 CREATE TABLE 时失败。

示例：

```hocon
sink {
  StarRocks {
    base-url = "jdbc:mysql://127.0.0.1:9030"
    nodeUrls = ["127.0.0.1:8030"]
    username = "root"
    password = ""
    database = "test"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    table_options = {
      replication_num = "3"
      storage_format = "V2"
    }
  }
}
```

### Zeta 定时刷新

该引擎级能力仅由 Zeta 支持。可以在 `env` 块中配置 `sink.flush.interval`，使尚未达到 `batch_max_rows` 和 `batch_max_bytes` 的缓冲数据也能定时通过 StarRocks Stream Load 写出。Spark 和 Flink 不会触发该定时刷新。

:::tip

StarRocks 定时刷新不提供基于 2PC 的精准一次语义，StarRocks Sink 仍为至少一次语义，任务重启后可能重复提交数据。如果业务场景适用，可以使用具有确定性主键的 Primary Key 表吸收重复写入。

:::

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 300000
  sink.flush.interval = 5000
}

sink {
  StarRocks {
    nodeUrls = ["starrocks-fe:8030"]
    base-url = "jdbc:mysql://starrocks-fe:9030/mydb"
    username = root
    password = ""
    database = "mydb"
    table = "mytable"
    batch_max_rows = 10000
    batch_max_bytes = 104857600
  }
}
```

## 数据类型映射

| StarRocks数据类型 | SeaTunnel数据类型 |
|---------------|---------------|
| BOOLEAN       | BOOLEAN       |
| TINYINT       | TINYINT       |
| SMALLINT      | SMALLINT      |
| INT           | INT           |
| BIGINT        | BIGINT        |
| FLOAT         | FLOAT         |
| DOUBLE        | DOUBLE        |
| DECIMAL       | DECIMAL       |
| DATE          | STRING        |
| TIME          | STRING        |
| DATETIME      | STRING        |
| STRING        | STRING        |
| ARRAY         | STRING        |
| MAP           | STRING        |
| BYTES         | STRING        |

#### 支持导入的数据格式

StarRocks数据接收器支持的格式有CSV和JSON格式。

## 任务示例

### 简单示例

> 接下来给出一个示例，该示例包含多种数据类型的数据写入，且用户需要为目标端下游创建相应表

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval = 10000
}

source {
  FakeSource {
    row.num = 10
    map.size = 10
    array.size = 10
    bytes.length = 10
    string.length = 10
    schema = {
      fields {
        c_map = "map<string, array<int>>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(16, 1)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
    }
}

sink {
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    base-url = "jdbc:mysql://e2e_starRocksdb:9030/"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    batch_max_rows = 10
    starrocks.config = {
      format = "JSON"
      strip_outer_array = true
    }
  }
}
```

### 支持写入cdc变更事件（INSERT/UPDATE/DELETE）示例

```hocon
sink {
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    base-url = "jdbc:mysql://e2e_starRocksdb:9030/"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    ...
    
    // 支持upsert/delete事件的同步（需要将选项参数enable_upsert_delete设置为true），仅支持表引擎为主键模型
    enable_upsert_delete = true
  }
}
```

### JSON格式数据导入示例

```
sink {
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    base-url = "jdbc:mysql://e2e_starRocksdb:9030/"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    batch_max_rows = 10
    starrocks.config = {
      format = "JSON"
      strip_outer_array = true
    }
  }
}

```

### CSV格式数据导入示例

```
sink {
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    base-url = "jdbc:mysql://e2e_starRocksdb:9030/"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    batch_max_rows = 10
    starrocks.config = {
      format = "CSV"
      column_separator = "\\x01"
      row_delimiter = "\\x02"
    }
  }
}
```

### 使用save_mode的示例

```
sink {
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    base-url = "jdbc:mysql://e2e_starRocksdb:9030/"
    username = root
    password = ""
    database = "test"
    table = "test_${schema_name}_${table_name}"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode="APPEND_DATA"
    batch_max_rows = 10
    starrocks.config = {
      format = "CSV"
      column_separator = "\\x01"
      row_delimiter = "\\x02"
    }
  }
}
```

## 常见问题

### StarRocks Sink 支持自动建表吗？

支持。通过 `schema_save_mode` 参数控制建表行为：

- `CREATE_SCHEMA_WHEN_NOT_EXIST`：表不存在时创建，已存在则跳过。
- `RECREATE_SCHEMA`：每次任务启动时删除并重建表。
- `ERROR_WHEN_SCHEMA_NOT_EXIST`：表不存在时抛出异常。
- `IGNORE`：跳过所有建表逻辑。

SeaTunnel 会根据上游 schema 自动推断 StarRocks 列类型。

### StarRocks Sink 是否支持 Upsert 和 DELETE 操作？

支持。设置 `enable_upsert_delete = true` 可以传播 Upsert 和 DELETE 事件，目标 StarRocks 表必须使用**主键模型（Primary Key）**。来自 CDC 数据源的 DELETE 事件在开启此选项后可正确传播。

### StarRocks Sink 中的 `labelPrefix` 是做什么的？

当前 StarRocks Sink 页面并未将精确一次列为已支持的 Connector 能力。
`labelPrefix` 用于控制 Sink 生成的 Stream Load label 前缀，保持此前缀稳定且全局唯一，
可以减少重试或任务重启时的 label 冲突：

```hocon
sink {
  StarRocks {
    nodeUrls = ["starrocks-fe:8030"]
    base-url = "jdbc:mysql://starrocks-fe:9030/"
    username = root
    password = ""
    database = "mydb"
    table = "mytable"
    labelPrefix = "unique-job-label"
  }
}
```

正式契约请以本页的**主要特性**矩阵和 `labelPrefix` option 说明为准。

### StarRocks 列名是否区分大小写？

StarRocks 列名默认不区分大小写。请确认上游字段名与目标 StarRocks 列名的映射关系，避免意外的字段不匹配。

### `nodeUrls` 和 `base-url` 有什么区别？

- `nodeUrls`：StarRocks FE 节点的 HTTP 地址，用于 Stream Load 数据写入。
- `base-url`：指向 StarRocks FE 节点的 JDBC URL，用于建表、查询 schema 等 DDL 操作。

开启自动建表时两者均需配置。

## 变更日志

<ChangeLog />
