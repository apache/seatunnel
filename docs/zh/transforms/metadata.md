# 元数据提取

> Metadata：把库名、表名、RowKind 等元数据提取为普通字段

## 描述

Metadata 转换插件用于将数据行中的元数据信息提取为普通字段，方便后续处理和分析。

**核心功能：**
- 将元数据（如数据库名、表名、行类型等）提取为可见字段
- 支持自定义输出字段名称
- 不改变原有数据字段，只是新增元数据字段

**典型应用场景：**
- CDC 数据同步时需要记录数据来源（库名、表名）
- 需要追踪数据变更类型（INSERT、UPDATE、DELETE）
- 需要记录数据的事件时间和延迟信息
- 多表合并时需要标识数据来源

## 支持的元数据字段

|    元数据Key    | 输出类型 |          说明          | 数据来源 |
|:---------:|:--------:|:-----------------------------:|:----:|
| Database  |  string  |  数据所属的数据库名称  | 所有连接器 |
|   Table   |  string  |  数据所属的表名称  | 所有连接器 |
|  RowKind  |  string  |  行的变更类型，值为：+I（插入）、-U（更新前）、+U（更新后）、-D（删除）  | 所有连接器 |
| EventTime | long   | 数据变更的事件时间戳（毫秒） | CDC 连接器；Kafka 源（ConsumerRecord.timestamp） |
|   Delay   |   long   |  数据采集延迟时间（毫秒），即数据抽取时间与数据库变更时间的差值  | CDC 连接器 |
| SourceTimestamp | long | 数据在源数据库中提交的时间戳（毫秒，即 `source.ts_ms`）。 | CDC 连接器 |
| BinlogFile | string | Binlog 文件名（如 `mysql-bin-changelog.000123`）。快照行返回 `null`。 | 仅 MySQL-CDC |
| BinlogPos  | long   | Binlog 字节偏移量。快照行返回 `null`。 | 仅 MySQL-CDC |
| BinlogRow  | int    | Binlog 事件中的行索引（从 0 开始）。快照行返回 `null`。 | 仅 MySQL-CDC |
| Gtid       | string | 全局事务 ID（格式：`server_uuid:transaction_id`）。GTID 未启用或快照行时返回 `null`。 | 仅 MySQL-CDC |
| Partition |  string  |  数据所属的分区信息，多个分区字段使用逗号分隔  | 支持分区的连接器 |

## Knowledge Sync 元数据字段

Knowledge Sync 流程可以使用下面的逻辑元数据 Key 携带文档和 chunk 身份信息。这些 Key 只有通过 `Metadata` 转换显式投影后，才会成为真实的物理列。

`Metadata` 转换本身不会生成 Knowledge Sync 元数据。上游 Source 或 Transform 必须先在 `CatalogTable.metadataSchema` 中声明这些元数据字段，并将对应值写入 `SeaTunnelRow.options`。

| 元数据 Key | 标准物理字段名 | 输出类型 | 说明 |
|:---:|:---:|:---:|:---|
| DocumentId | `document_id` | string | 每个文档生命周期事件都必须包含的非空稳定文档标识。 |
| DocumentHash | `document_hash` | string | 稳定的文档版本或内容哈希。 |
| SourceUri | `source_uri` | string | 不包含凭证的稳定来源 URI 或路径。 |
| SourceVersion | `source_version` | string | 来源侧版本、etag、revision 等版本标记。 |
| SourceModifiedAt | `source_modified_at` | long | 来源修改时间，epoch 毫秒。 |
| MimeType | `mime_type` | string | 来源 MIME 类型。 |
| Deleted | `deleted` | boolean | 非空生命周期标记：普通行为 `false`，文档 tombstone 为 `true`。 |
| ChunkId | `chunk_id` | string | 稳定的 chunk 标识。普通 chunk 行必填，文档 tombstone 可为 `null`。 |
| ChunkHash | `chunk_hash` | string | 稳定的 chunk 内容哈希。普通 chunk 行必填，文档 tombstone 可为 `null`。 |
| ChunkIndex | `chunk_index` | int | 文档内从 0 开始的 chunk 序号。普通 chunk 行必填，文档 tombstone 可为 `null`。 |

### 重要说明

1. **元数据字段区分大小写**：配置时必须严格按照上表中的 Key 名称（如 `Database`、`Table`、`RowKind` 等）。
2. **时间相关字段**：`Delay` 和 `SourceTimestamp` 仅在 CDC 连接器有效。`EventTime` 也会在 Kafka 源中使用 `ConsumerRecord.timestamp`（毫秒，非负时）写入。
3. **Kafka 事件时间**：Kafka 源会在 `ConsumerRecord.timestamp` 非负时写入 `EventTime`，可通过 Metadata 转换将其暴露为普通字段。
4. **Binlog/GTID 字段**：`BinlogFile`、`BinlogPos`、`BinlogRow`、`Gtid` 仅适用于 MySQL-CDC。使用 `startup.mode = initial` 时，快照行的这四个字段均为 `null`。
5. **Knowledge Sync 投影需要显式配置**：Knowledge Sync 元数据字段只有在 `metadata_fields` 中配置、输入表 metadata schema 中声明了对应 Key，并且行 options 中存在对应值时才会被投影。该转换读取的是逻辑行元数据，不会读取同名的已有物理列。
6. **兼容 Markdown RAG 物理字段**：当 `markdown_rag_metadata_enabled=true` 时，Markdown 除现有物理列 `source_uri`、`document_id`、`chunk_id`、`chunk_index`、`content_hash` 外，还会声明并输出逻辑 `SourceUri`、`DocumentId`、`DocumentHash`、`ChunkHash`。物理字段的名称、顺序、值、公式和路由行为均保持不变。
7. **来源 URI 安全**：producer 在将 `SourceUri` 写入行 options 前，必须移除 URI userinfo、访问令牌、签名以及其他临时鉴权信息。通用 Markdown bridge 会移除分层远程 URI 的完整 query 和 fragment；如果来源身份依赖 query，则必须提供稳定且不敏感的 path。
8. **Knowledge Sync 可空语义**：`DocumentId` 用于标识每个文档生命周期事件。声明 `Deleted` 后，producer 必须为普通行写入 `false`，为文档 tombstone 写入 `true`，不能使用 `null`。普通 chunk 行必须包含 `ChunkId`、`ChunkHash` 和 `ChunkIndex`；紧凑的文档 tombstone 可以将这些 chunk 字段留为 `null`。
9. **投影冲突**：输出字段名不能与已有物理字段重复。启用 Markdown RAG metadata 后已经存在物理 `source_uri` 和 `document_id`，因此应把逻辑值投影为 `ks_source_uri`、`ks_document_id` 等别名。

### Markdown Source Bridge

当 `file_format_type=markdown` 且 `markdown_rag_metadata_enabled=true` 时，Markdown source 会成为 Knowledge Sync 元数据 producer。

| 逻辑 Key | Markdown 值 |
|:---:|:---|
| SourceUri | 本地路径沿用现有归一化。分层远程 URI 保留转为小写的 scheme 和 host、显式端口及 path，同时移除 user info、query 和 fragment。 |
| DocumentId | `doc_` 加逻辑 `SourceUri` 的 UTF-8 字节的小写 SHA-256。 |
| DocumentHash | UTF-8 解码和 Markdown 解析前实际读取到的精确来源字节的小写 SHA-256。 |
| ChunkHash | 当前输出行 `text` 的 UTF-8 字节的小写 SHA-256，null 按空字符串处理。在 Markdown source 边界，其值等于物理 `content_hash`。 |

逻辑身份与兼容身份相互独立。对于带签名或凭据的远程 URL，逻辑 `SourceUri`、`DocumentId` 可能与物理 `source_uri`、`document_id` 不同；物理值和 split 路由公式保持不变。

请为两个发生冲突的身份字段配置别名：

```hocon
source {
  LocalFile {
    plugin_output = "markdown_rows"
    path = "/data/knowledge"
    file_format_type = "markdown"
    markdown_rag_metadata_enabled = true
  }
}

transform {
  Metadata {
    plugin_input = "markdown_rows"
    plugin_output = "markdown_rows_with_logical_metadata"
    metadata_fields = {
      SourceUri = "ks_source_uri"
      DocumentId = "ks_document_id"
      DocumentHash = "document_hash"
      ChunkHash = "chunk_hash"
    }
  }
}
```

`ChunkHash` 只对 Markdown 直接输出的当前行有效。如果下游 transform 修改 `text` 或把一行展开为多个 chunk，则必须在把行发送到 lifecycle sink 前重新计算最终 `ChunkHash`、`ChunkId` 和 `ChunkIndex`。该 bridge 仅完成 producer 集成，不实现增量比较、writer affinity、过期 chunk 删除或 tombstone。

## 配置选项

|      参数名       | 类型 | 是否必填 | 默认值 | 说明       |
|:---------------:|------|:--------:|:-------------:|-------------------|
| metadata_fields | map  |    否     |   空映射   | 元数据字段与输出字段的映射关系，格式为 `元数据Key = 输出字段名` |

### metadata_fields [map]

定义元数据字段到输出字段的映射关系。

**配置格式：**
```hocon
metadata_fields {
  <元数据Key> = <输出字段名>
  <元数据Key> = <输出字段名>
  ...
}
```

**配置示例：**
```hocon
metadata_fields {
  Database = source_db      # 将数据库名映射到 source_db 字段
  Table = source_table      # 将表名映射到 source_table 字段
  RowKind = op_type         # 将行类型映射到 op_type 字段
  EventTime = event_ts      # 将事件时间映射到 event_ts 字段
  Delay = sync_delay        # 将延迟时间映射到 sync_delay 字段
  Partition = partition_info # 将分区信息映射到 partition_info 字段
}
```

**注意事项：**
- 左侧必须是支持的元数据 Key（见上表），且严格区分大小写
- 右侧是自定义的输出字段名，不能与原有字段重名
- 可以只选择需要的元数据字段，不必全部配置

### Knowledge Sync 投影示例

将 Knowledge Sync 逻辑元数据 Key 投影为标准物理列。上游 producer 必须已经通过行 options 提供这些元数据值，并在表 metadata schema 中声明这些字段。

```hocon
transform {
  Metadata {
    plugin_input = "knowledge_chunks"
    plugin_output = "knowledge_chunks_with_meta"
    metadata_fields = {
      DocumentId = "document_id"
      DocumentHash = "document_hash"
      ChunkId = "chunk_id"
      ChunkHash = "chunk_hash"
      ChunkIndex = "chunk_index"
    }
  }
}
```

完成该转换后，下游组件可以把 `document_id`、`chunk_id`、`chunk_hash` 当作输入 schema 中的普通物理字段读取。

## 完整示例

### 示例 1：MySQL CDC 数据同步，提取所有元数据

从 MySQL 数据库同步数据，并提取所有可用的元数据信息。

```yaml
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    plugin_output = "mysql_cdc_source"
    server-id = 5652
    username = "root"
    password = "your_password"
    table-names = ["mydb.users"]
    url = "jdbc:mysql://localhost:3306/mydb"
  }
}

transform {
  Metadata {
    plugin_input = "mysql_cdc_source"
    plugin_output = "metadata_added"
    metadata_fields {
      Database = source_database    # 提取数据库名
      Table = source_table          # 提取表名
      RowKind = change_type         # 提取变更类型
      EventTime = event_timestamp   # 提取事件时间
      Delay = sync_delay_ms         # 提取同步延迟
    }
  }
}

sink {
  Console {
    plugin_input = "metadata_added"
  }
}
```

**输入数据示例：**
```
原始数据行（来自 mydb.users 表）：
id=1, name="张三", age=25
RowKind: +I (INSERT)
```

**输出数据示例：**
```
转换后的数据行：
id=1, name="张三", age=25, source_database="mydb", source_table="users",
change_type="+I", event_timestamp=1699000000000, sync_delay_ms=100
```

---

### 示例 2：只提取部分元数据

只提取数据来源信息（库名和表名），用于多表合并场景。

```yaml
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  MySQL-CDC {
    plugin_output = "multi_table_source"
    server-id = 5652
    username = "root"
    password = "your_password"
    table-names = ["db1.orders", "db2.orders"]
    url = "jdbc:mysql://localhost:3306"
  }
}

transform {
  Metadata {
    plugin_input = "multi_table_source"
    plugin_output = "with_source_info"
    metadata_fields {
      Database = db_name
      Table = table_name
    }
  }
}

sink {
  Jdbc {
    plugin_input = "with_source_info"
    url = "jdbc:mysql://localhost:3306/target_db"
    table = "merged_orders"
    # 目标表会包含 db_name 和 table_name 字段，用于标识数据来源
  }
}
```

### 示例 3：Kafka 写入时间用于分区

将 Kafka `ConsumerRecord.timestamp`（写入到 `EventTime` 元数据）暴露为普通字段，再生成分区字段并写入 Hive，适合回放或补数场景。

```hocon
env {
  execution.parallelism = 4
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Kafka {
    plugin_output = "kafka_raw"
    schema = {
      fields {
        id = bigint
        customer_type = string
        data = string
      }
    }
    format = text
    field_delimiter = "|"
    topic = "push_report_event"
    bootstrap.servers = "kafka-broker-1:9092,kafka-broker-2:9092"
    consumer.group = "seatunnel_event_backfill"
    kafka.config = {
      max.poll.records = 100
      auto.offset.reset = "earliest"
      enable.auto.commit = "false"
    }
  }
}

transform {
  Metadata {
    plugin_input = "kafka_raw"
    plugin_output = "kafka_with_meta"
    metadata_fields = {
      EventTime = "kafka_ts"
    }
  }

  Sql {
    plugin_input = "kafka_with_meta"
    plugin_output = "source_table"
    query = "select id, customer_type, data, FROM_UNIXTIME(kafka_ts/1000, 'yyyy-MM-dd', 'Asia/Shanghai') as pt from kafka_with_meta where kafka_ts >= 0"
  }
}

sink {
  Hive {
    table_name = "example_db.ods_sys_event_report"
    metastore_uri = "thrift://metastore-1:9083,thrift://metastore-2:9083"
    hdfs_site_path = "/path/to/hdfs-site.xml"
    hive_site_path = "/path/to/hive-site.xml"
    krb5_path = "/path/to/krb5.conf"
    kerberos_principal = "hive/metastore-1@EXAMPLE.COM"
    kerberos_keytab_path = "/path/to/hive.keytab"
    overwrite = false
    plugin_input = "source_table"
    # compress_codec = "SNAPPY"
  }
}
```

上面的 `pt` 字段由 Kafka 事件时间转换而来，可在 Hive 中作为分区列使用，便于补数和校准分区。

### 示例 4：结合 Metadata 和 Sql 提取分表后缀并生成装载日期

当上游是按月或按天分表的 CDC 源时，常见需求是先把 `Table` 元数据暴露成普通字段，再用
`Sql` 提取分表后缀、补充任务装载日期。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  MySQL-CDC {
    plugin_output = "orders_cdc"
    server-id = 5652
    username = "root"
    password = "your_password"
    table-names = ["app.orders_202401", "app.orders_202402"]
    url = "jdbc:mysql://localhost:3306/app"
  }
}

transform {
  Metadata {
    plugin_input = "orders_cdc"
    plugin_output = "orders_with_meta"
    metadata_fields {
      Table = source_table
      EventTime = event_ts
    }
  }

  Sql {
    plugin_input = "orders_with_meta"
    plugin_output = "orders_normalized"
    query = "select id, amount, source_table, REGEXP_SUBSTR(source_table, '[0-9]+$') as table_suffix, FROM_UNIXTIME(event_ts / 1000, 'yyyy-MM-dd HH:mm:ss', 'Asia/Shanghai') as event_time_str, FORMATDATETIME(CURRENT_TIMESTAMP, 'yyyyMMdd') as load_date from orders_with_meta"
  }
}

sink {
  Console {
    plugin_input = "orders_normalized"
  }
}
```

如果当前记录来自 `orders_202402`，那么：

- `source_table = "orders_202402"`
- `table_suffix = "202402"`
- `event_time_str` 来自 CDC 事件时间
- `load_date` 是任务运行时格式化后的日期字符串
