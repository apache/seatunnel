# MaxWell 格式

[Maxwell](https://maxwells-daemon.io/) 是一个 CDC（变更数据捕获）工具，能够实时捕获 MySQL 的数据变化并将其流式传输到 Kafka、Kinesis 和其他流连接器中。Maxwell 为变更日志提供了一种统一的格式，并支持使用 JSON 进行消息的序列化。

SeaTunnel 能够解析 Maxwell 的 JSON 消息，并将其转化为 INSERT/UPDATE/DELETE 消息，进而输入到 SeaTunnel 系统中。这个特性在很多场景下都显得非常有用，例如：

        从数据库同步增量数据到其他系统
        审计日志
        数据库的实时物化视图
        关联维度数据库的变更历史，等等。

SeaTunnel 还支持将 SeaTunnel 中的 INSERT/UPDATE/DELETE 消息编码为 Maxwell JSON 消息，并将其发送到类似 Kafka 这样的存储中。然而，目前 SeaTunnel 无法将 UPDATE_BEFORE 和 UPDATE_AFTER 合并为一个单一的 UPDATE 消息。因此，SeaTunnel 将 UPDATE_BEFORE 和 UPDATE_AFTER 编码为 Maxwell 的 DELETE 和 INSERT 消息。

## 限制

更新消息必须包含完整行数据；仅包含变更字段的部分更新暂不支持。

# 格式选项

| 选项 | 默认值 | 是否需要 | 描述 |
|------|--------|--------|------|
| format | json | 是 | 指定要使用的格式，这里应该是 `maxwell_json`。 |

> **注意：** 带前缀的 `maxwell_json.ignore-parse-errors`、`maxwell_json.database.include` 和 `maxwell_json.table.include` 不是有效的配置项，配置后不会生效。是否跳过无法解析的记录由各连接器自行决定（例如 Kafka 读取 Maxwell JSON 时始终跳过），format 模块的 `database.include`/`table.include` 过滤器目前也未接入任何连接器。

# 如何使用 Maxwell 格式

## Kafka 使用示例

Maxwell 为变更日志提供了一种统一的格式，以下是一个从 MySQL products 表捕获的变更操作的简单示例：

```bash
{
    "database":"test",
    "table":"product",
    "type":"insert",
    "ts":1596684904,
    "xid":7201,
    "commit":true,
    "data":{
        "id":111,
        "name":"scooter",
        "description":"Big 2-wheel scooter ",
        "weight":5.18
    },
    "primary_key_columns":[
        "id"
    ]
}
```

注意：请参考 Maxwell 文档了解每个字段的含义。

MySQL products 表有 4 列（id、name、description 和 weight）。
上面的 JSON 消息是 products 表上的一个更新变更事件，其中 id = 111 的行的 weight 值从 5.18 更改为 5.15。
假设消息已同步到 Kafka 主题 products_binlog，那么我们可以使用以下 SeaTunnel 来消费此主题并解释变更事件。

```bash
env {
    execution.parallelism = 1
    job.mode = "BATCH"
}

source {
  Kafka {
    bootstrap.servers = "kafkaCluster:9092"
    topic = "products_binlog"
    plugin_output = "kafka_name"
    start_mode = earliest
    schema = {
      fields {
           id = "int"
           name = "string"
           description = "string"
           weight = "string"
      }
    },
    format = maxwell_json
  }

}

transform {
}

sink {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "consume-binlog"
    format = maxwell_json
  }
}
```

