# Canal 格式

变更数据捕获格式:
序列化模式、反序列化模式

Canal是一款CDC（变更数据捕获）工具，能够实时捕获MySQL的数据变化并将其流式传输到其他系统中。Canal为变更日志提供了一种统一的格式，并支持使用 JSON 和 protobuf（Canal默认使用protobuf）进行消息的序列化

SeaTunnel 能够解析 Canal 的 JSON 消息，并将其转化为 INSERT/UPDATE/DELETE 消息，进而输入到 SeaTunnel 系统中。这个特性在很多场景下都显得非常有用，例如:

        将增量数据从数据库同步到其他系统
        审计日志
        数据库的实时物化视图
        关联维度数据库的变更历史，等等。

SeaTunnel 还支持将 SeaTunnel 中的 INSERT/UPDATE/DELETE 消息编码为 Canal JSON 消息，并将其发送到类似 Kafka 这样的存储中。然而，目前 SeaTunnel 无法将 UPDATE_BEFORE 和 UPDATE_AFTER 合并为一个单一的UPDATE消息。因此，SeaTunnel将 UPDATE_BEFORE 和 UPDATE_AFTER 编码为 Canal的 DELETE 和 INSERT 消息来进行

# 格式选项

|               选项               |  默认值   | 是否需要 |                                         描述                                         |
|--------------------------------|--------|------|------------------------------------------------------------------------------------|
| format                         | (none) | 是    | 指定要使用的格式，这里应该是 `canal_json`                                                        |
| canal_json.ignore-parse-errors | false  | 否    | 跳过解析错误的字段和行，而不是失败。出现错误的字段将被设置为null                                                 |
| canal_json.database.include    | (none) | 否    | 正则表达式，可选，通过正则匹配 Canal 记录中的`database`元字段来仅读取特定数据库变更日志行。此字符串Pattern模式与Java的Pattern兼容 |
| canal_json.table.include       | (none) | 否    | 正则表达式，可选，通过正则匹配 Canal 记录中的`table`元字段来仅读取特定数据库变更日志行。此字符串Pattern模式与Java的Pattern兼容    |

# 如何使用

## Kafka 使用示例

Canal为变更日志提供了一种统一的格式，以下是一个从MySQL products 表捕获的变更操作的简单示例

```bash
{
  "data": [
    {
      "id": "111",
      "name": "scooter",
      "description": "Big 2-wheel scooter",
      "weight": "5.18"
    }
  ],
  "database": "inventory",
  "es": 1589373560000,
  "id": 9,
  "isDdl": false,
  "mysqlType": {
    "id": "INTEGER",
    "name": "VARCHAR(255)",
    "description": "VARCHAR(512)",
    "weight": "FLOAT"
  },
  "old": [
    {
      "weight": "5.15"
    }
  ],
  "pkNames": [
    "id"
  ],
  "sql": "",
  "sqlType": {
    "id": 4,
    "name": 12,
    "description": 12,
    "weight": 7
  },
  "table": "products",
  "ts": 1589373560798,
  "type": "UPDATE"
}
```

注：请参考 [Canal 文档](https://github.com/alibaba/canal/wiki) 以了解每个字段的含义

MySQL 的 products 表有 4 列（id、name、description 和 weight）
上述 JSON 消息是产品表的一个更新变更事件，其中 id = 111 的行的 weight 值从 5.15 变为 5.18
假设此表的 binlog 的消息已经同步到 Kafka topic，那么我们可以使用下面的 SeaTunnel 示例来消费这个主题并体现变更事件

```bash
env {
    parallelism = 1
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
    format = canal_json
  }

}

transform {
}

sink {
  Kafka {
    bootstrap.servers = "localhost:9092"
    topic = "consume-binlog"
    format = canal_json
  }
}
```

