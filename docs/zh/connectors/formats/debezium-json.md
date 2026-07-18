# Debezium 格式

变更数据捕获格式:
序列化模式、反序列化模式

Debezium 是一套分布式服务，用于捕获数据库中的变化，以便您的应用程序可以看到这些变化并对其做出响应。Debezium 在变更事件流中记录每个数据库表中的所有行级变化，应用程序只需读取这些流，就可以按照它们发生的顺序看到变更事件

SeaTunnel 支持将 Debezium JSON 消息解析为 INSERT/UPDATE/DELETE 消息并导入到 seatunnel 系统中。在许多情况下，利用这个特性是非常有用的，例如:

        将增量数据从数据库同步到其他系统
        审计日志
        数据库的实时物化视图
        关联维度数据库的变更历史，等等。

SeaTunnel 还支持将 SeaTunnel 中的 INSERT/UPDATE/DELETE 消息解析为 Debezium JSON 消息，并将其发送到类似 Kafka 这样的存储中

# 格式选项

|                选项                 |  默认值   | 是否需要 |                  描述                  |
|-----------------------------------|--------|------|--------------------------------------|
| format                            | (none) | 是    | 指定要使用的格式，这里应该是 'debezium_json'.      |
| debezium-json.ignore-parse-errors | false  | 否    | 跳过有解析错误的字段和行而不是失败。如果出现错误，字段将设置为 null |

# 如何使用

## Kafka 使用示例

Debezium 提供了一个统一的变更日志格式，下面是一个 MySQL products 表捕获的变更操作的简单示例

```bash
{
	"before": {
		"id": 111,
		"name": "scooter",
		"description": "Big 2-wheel scooter ",
		"weight": 5.18
	},
	"after": {
		"id": 111,
		"name": "scooter",
		"description": "Big 2-wheel scooter ",
		"weight": 5.17
	},
	"source": {
		"version": "1.1.1.Final",
		"connector": "mysql",
		"name": "dbserver1",
		"ts_ms": 1589362330000,
		"snapshot": "false",
		"db": "inventory",
		"table": "products",
		"server_id": 223344,
		"gtid": null,
		"file": "mysql-bin.000003",
		"pos": 2090,
		"row": 0,
		"thread": 2,
		"query": null
	},
	"op": "u",
	"ts_ms": 1589362330904,
	"transaction": null
}
```

注：请参考 [Debezium 文档](https://github.com/debezium/debezium/blob/v1.9.8.Final/documentation/modules/ROOT/pages/connectors/mysql.adoc#data-change-events) 以了解每个字段的含义

MySQL 的 products 表有 4 列（id、name、description 和 weight）
上述 JSON 消息是产品表的一个更新变更事件，其中 id = 111 的行的 weight 值从 5.18 变为 5.17
假设消息已经同步到 Kafka 主题 products_binlog，那么我们可以使用以下的 SeaTunnel 配置来消费这个主题并通过 Debezium 格式解释变更事件。

在此配置中，您必须指定 `schema` 和 `debezium_record_include_schema` 选项：
- `schema` 应与您的表格式相同
- 如果您的 json 数据包含 `schema` 字段，`debezium_record_include_schema` 应为 true，如果您的 json 数据不包含 `schema` 字段，`debezium_record_include_schema` 应为 false
- `{"schema" : {}, "payload": { "before" : {}, "after": {} ... } }` --> `true`
- `{"before" : {}, "after": {} ... }` --> `false`"

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
    }
    debezium_record_include_schema = false
    format = debezium_json
  }

}

transform {
}

sink {
  Kafka {
    bootstrap.servers = "kafkaCluster:9092"
    topic = "consume-binlog"
    format = debezium_json
  }
}
```

