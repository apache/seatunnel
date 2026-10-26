---
sidebar_position: 3
title: Kafka 到 Iceberg
---

# Kafka 到 Iceberg

当你想把 Kafka 里的流式事件落到 Iceberg 表中，供后续分析查询使用时，可以使用这条链路。

## 前置条件

1. 先完成 [跑第一个任务](../locally/run-your-first-job.md)。

2. 安装这条链路需要的插件。先看 [部署 > 下载连接器插件](../locally/deployment.md#下载连接器插件)，然后把 `config/plugin_config` 改成下面这样：

```plugin_config
--seatunnel-connectors--
connector-kafka
connector-iceberg
--end--
```

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
ls connectors | rg 'connector-(kafka|iceberg)'
```

3. 如果你使用 Flink 或 Spark，请补齐 Iceberg 在对应环境里需要的依赖，例如 `hive-exec` 和 `libfb303`。

4. 先选一个空的、当前进程可写的 Iceberg warehouse 目录。这篇教程使用本地 Hadoop catalog，对应路径是 `file:///tmp/seatunnel/iceberg/warehouse-demo`：

```bash
mkdir -p /tmp/seatunnel/iceberg/warehouse-demo
```

5. 在启动任务前，先创建 Kafka topic 并写入几条 JSON 消息：

```bash
kafka-topics.sh \
  --create \
  --if-not-exists \
  --topic orders \
  --bootstrap-server kafka:9092 \
  --partitions 1 \
  --replication-factor 1

kafka-console-producer.sh --topic orders --bootstrap-server kafka:9092 <<'EOF'
{"id":1001,"customer_id":2001,"total_amount":19.99,"event_date":"2026-06-12"}
{"id":1002,"customer_id":2002,"total_amount":29.99,"event_date":"2026-06-12"}
EOF
```

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
      warehouse = "file:///tmp/seatunnel/iceberg/warehouse-demo"
    }
    iceberg.table.primary-keys = "id"
    iceberg.table.partition-keys = "event_date"
    iceberg.table.upsert-mode-enabled = true
    iceberg.table.schema-evolution-enabled = true
    case_sensitive = true
  }
}
```

## 运行任务

把配置保存为 `config/kafka-to-iceberg.conf`，然后用本地模式启动 SeaTunnel：

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/kafka-to-iceberg.conf -m local
```

这是一条流式任务，所以 Kafka 消息被消费和落表时，任务需要保持运行中。

## 验证结果

1. 检查 warehouse 路径下是否生成了 Iceberg 元数据和数据文件。
2. 使用 Spark、Trino 或其他 Iceberg 兼容引擎查询表。

```bash
ls /tmp/seatunnel/iceberg/warehouse-demo/lakehouse/orders
spark-sql \
  --conf spark.sql.catalog.seatunnel_demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.seatunnel_demo.type=hadoop \
  --conf spark.sql.catalog.seatunnel_demo.warehouse=file:///tmp/seatunnel/iceberg/warehouse-demo \
  -e "SELECT COUNT(*) FROM seatunnel_demo.lakehouse.orders"
```

如果表可以正常查询，且行数和你写入 Kafka 的消息数量一致，这条链路就是通的。按照上面两条样例消息，最终行数应该是 `2`。

## 常见坑

- Kafka 中的 JSON 消息结构和 source 里定义的 schema 不一致。
- 流作业没有开启 checkpoint，导致重启和一致性行为变弱。
- Iceberg catalog 类型对了，但 warehouse 路径对当前引擎进程不可写。
- 开启了 upsert 模式，但消息并没有稳定主键。

## 相关文档

- [Kafka Source](../../connectors/source/Kafka.md)
- [Iceberg Sink](../../connectors/sink/Iceberg.md)
