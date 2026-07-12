# MaxWell 格式

[Maxwell](https://maxwells-daemon.io/) 是一个 CDC（变更数据捕获）工具，能够实时捕获 MySQL 的数据变化并将其流式传输到 Kafka、Kinesis 和其他流连接器中。Maxwell 为变更日志提供了一种统一的格式，并支持使用 JSON 进行消息的序列化。

SeaTunnel 能够解析 Maxwell 的 JSON 消息，并将其转化为 INSERT/UPDATE/DELETE 消息，进而输入到 SeaTunnel 系统中。这个特性在很多场景下都显得非常有用，例如：

        从数据库同步增量数据到其他系统
        审计日志
        数据库的实时物化视图
        关联维度数据库的变更历史，等等。

SeaTunnel 还支持将 SeaTunnel 中的 INSERT/UPDATE/DELETE 消息编码为 Maxwell JSON 消息，并将其发送到类似 Kafka 这样的存储中。然而，目前 SeaTunnel 无法将 UPDATE_BEFORE 和 UPDATE_AFTER 合并为一个单一的 UPDATE 消息。因此，SeaTunnel 将 UPDATE_BEFORE 和 UPDATE_AFTER 编码为 Maxwell 的 DELETE 和 INSERT 消息。

# 格式选项

| 选项 | 默认值 | 是否需要 | 描述 |
|------|--------|--------|------|
| format | (none) | 是 | 指定要使用的格式，这里应该是 `maxwell_json`。 |
| maxwell_json.ignore-parse-errors | false | 否 | 跳过解析错误的字段和行，而不是失败。出现错误的字段将被设置为 null。 |
| maxwell_json.database.include | (none) | 否 | 正则表达式，可选，通过正则匹配 Maxwell 记录中的 `database` 元字段来仅读取特定数据库变更日志行。此字符串 Pattern 模式与 Java 的 Pattern 兼容。 |
| maxwell_json.table.include | (none) | 否 | 正则表达式，可选，通过正则匹配 Maxwell 记录中的 `table` 元字段来仅读取特定表的变更日志行。此字符串 Pattern 模式与 Java 的 Pattern 兼容。 |

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

