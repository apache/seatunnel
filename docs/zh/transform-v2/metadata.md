# Metadata

> Metadata 转换插件

## 描述

Metadata 转换插件用于将数据行中的元数据信息提取并转换为普通字段，方便后续处理和分析。

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
| Partition |  string  |  数据所属的分区信息，多个分区字段使用逗号分隔  | 支持分区的连接器 |

### 重要说明

1. **元数据字段区分大小写**：配置时必须严格按照上表中的 Key 名称（如 `Database`、`Table`、`RowKind` 等）。
2. **时间相关字段**：`Delay` 仅在 CDC 连接器有效（TiDB-CDC 除外）；`EventTime` 由 CDC 连接器写入，也会在 Kafka 源中使用 `ConsumerRecord.timestamp`（毫秒，非负时）写入。
3. **Kafka 事件时间**：Kafka 源会在 `ConsumerRecord.timestamp` 非负时写入 `EventTime`，可通过 Metadata 转换将其暴露为普通字段。

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
