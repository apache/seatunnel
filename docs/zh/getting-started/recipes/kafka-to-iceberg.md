---
sidebar_position: 3
title: Kafka 到 Iceberg
---

# Kafka 到 Iceberg

当你想把 Kafka 里的流式事件落到 Iceberg 表中，供后续分析查询使用时，可以使用这条链路。

## 前置条件

- 先完成 [跑第一个任务](../locally/run-your-first-job.md)。
- 安装 `connector-kafka` 和 `connector-iceberg`。
- 准备好一个包含 JSON 消息的 Kafka topic。
- 如果你使用 Flink 或 Spark，请补齐 Iceberg 在对应环境里需要的依赖，例如 `hive-exec` 和 `libfb303`。
- 先确定好 Iceberg catalog 类型。下面示例使用本地 Hadoop catalog，最适合本地验证。

## 最小配置

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Kafka {
    plugin_output = "orders_kafka"
    topic = "orders"
    bootstrap.servers = "kafka:9092"
    consumer.group = "seatunnel-orders"
    start_mode = "earliest"
    format = "json"
    schema = {
      fields {
        id = bigint
        customer_id = bigint
        total_amount = "decimal(16, 2)"
        event_date = string
      }
    }
  }
}

sink {
  Iceberg {
    plugin_input = "orders_kafka"
    catalog_name = "seatunnel_demo"
    namespace = "lakehouse"
    table = "orders"
    iceberg.catalog.config = {
      type = "hadoop"
      warehouse = "file:///tmp/seatunnel/iceberg/warehouse"
    }
    iceberg.table.primary-keys = "id"
    iceberg.table.partition-keys = "event_date"
    iceberg.table.upsert-mode-enabled = true
    iceberg.table.schema-evolution-enabled = true
    case_sensitive = true
  }
}
```

## 验证结果

1. 往 Kafka topic 中写入几条 JSON 消息。
2. 启动 SeaTunnel 作业。
3. 检查 warehouse 路径下是否生成了 Iceberg 元数据和数据文件。
4. 使用 Spark、Trino 或其他 Iceberg 兼容引擎查询表。

```bash
ls /tmp/seatunnel/iceberg/warehouse/lakehouse/orders
spark-sql \
  --conf spark.sql.catalog.seatunnel_demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.seatunnel_demo.type=hadoop \
  --conf spark.sql.catalog.seatunnel_demo.warehouse=file:///tmp/seatunnel/iceberg/warehouse \
  -e "SELECT COUNT(*) FROM seatunnel_demo.lakehouse.orders"
```

如果表可以正常查询，且行数和你写入 Kafka 的消息数量一致，这条链路就是通的。

## 常见坑

- Kafka 中的 JSON 消息结构和 source 里定义的 schema 不一致。
- 流作业没有开启 checkpoint，导致重启和一致性行为变弱。
- Iceberg catalog 类型对了，但 warehouse 路径对当前引擎进程不可写。
- 开启了 upsert 模式，但消息并没有稳定主键。

## 相关文档

- [Kafka Source](../../connectors/source/Kafka.md)
- [Iceberg Sink](../../connectors/sink/Iceberg.md)
