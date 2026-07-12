# Ogg 格式

[Oracle GoldenGate](https://www.oracle.com/integration/goldengate/) (a.k.a ogg) 是一项托管服务，提供实时数据网格平台，该平台使用复制来保持数据高度可用，并支持实时分析。客户可以设计、执行和监控其数据复制和流数据处理解决方案，而无需分配或管理计算环境。 Ogg 为变更日志提供了统一的格式结构，并支持使用 JSON 序列化消息。

SeaTunnel 支持将 Ogg JSON 消息解释为 Seatunnel 系统中的 INSERT/UPDATE/DELETE 消息。在许多情况下，这个特性带来了很多便利，例如

        将增量数据从数据库同步到其他系统
        审计日志
        数据库的实时物化视图
        关联维度数据库的变更历史，等等。

SeaTunnel 还支持将 SeaTunnel 中的 INSERT/UPDATE/DELETE 消息转化为 Ogg JSON 消息，并将其发送到类似 Kafka 这样的存储中。然而，目前 SeaTunnel 无法将 UPDATE_BEFORE 和 UPDATE_AFTER 组合成单个 UPDATE 消息。因此，Seatunnel 将 UPDATE_BEFORE 和 UPDATE_AFTER 转化为 DELETE 和 INSERT Ogg 消息来实现

# 格式选项

|              选项              |  默认值   | 是否需要 |                                         描述                                         |
|------------------------------|--------|------|------------------------------------------------------------------------------------|
| format                       | (none) | 是    | 指定要使用的格式，这里应该是`-json`                                                              |
| ogg_json.ignore-parse-errors | false  | 否    | 跳过有解析错误的字段和行而不是失败。如果出现错误，字段将设置为 null                                               |
| ogg_json.database.include    | (none) | 否    | 正则表达式，可选，通过正则匹配 Canal 记录中的`database`元字段来仅读取特定数据库变更日志行。此字符串Pattern模式与Java的Pattern兼容 |
| ogg_json.table.include       | (none) | 否    | 正则表达式，可选，通过正则匹配 Canal 记录中的 `table` 元字段来仅读取特定表的更改日志行。此字符串Pattern模式与Java的Pattern兼容   |

# 如何使用 Ogg 格式

## Kafka 使用示例

Ogg 为变更日志提供了统一的格式，下面是从 Oracle PRODUCTS 表捕获变更操作的简单示例：

```bash
{
  "before": {
    "id": 111,
    "name": "scooter",
    "description": "Big 2-wheel scooter",
    "weight": 5.18
  },
  "after": {
    "id": 111,
    "name": "scooter",
    "description": "Big 2-wheel scooter",
    "weight": 5.15
  },
  "op_type": "U",
  "op_ts": "2020-05-13 15:40:06.000000",
  "current_ts": "2020-05-13 15:40:07.000000",
  "primary_keys": [
    "id"
  ],
  "pos": "00000000000000000000143",
  "table": "PRODUCTS"
}
```

注：各字段含义请参考 [Debezium 文档](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/oracle.adoc#data-change-events)

此 Oracle PRODUCTS 表有 4 列 (id, name, description 和 weight)
上面的 JSON 消息是 products 表上的更新更改事件，其中 id = 111 的行的字段 `weight` 的值从 5.18 更改为 5.15。
假设此表的 binlog 的消息已经同步到 Kafka topic，那么我们可以使用下面的 SeaTunnel 示例来消费这个 topic 并体现变更事件。

```bash
env {
    parallelism = 1
    job.mode = "STREAMING"
}
source {
  Kafka {
    bootstrap.servers = "127.0.0.1:9092"
    topic = "ogg"
    plugin_output = "kafka_name"
    start_mode = earliest
    schema = {
      fields {
           id = "int"
           name = "string"
           description = "string"
           weight = "double"
      }
    },
    format = ogg_json
  }
}
sink {
    jdbc {
        url = "jdbc:mysql://127.0.0.1/test"
        driver = "com.mysql.cj.jdbc.Driver"
        user = "root"
        password = "12345678"
        table = "ogg"
        primary_keys = ["id"]
    }
}
```

