---
sidebar_position: 3
title: Kafka to Iceberg
---

# Kafka to Iceberg

Use this recipe when you want to land streaming events from Kafka into an Iceberg table for downstream analytics.

## Prerequisites

1. Finish [Run your first job](../locally/run-your-first-job.md).

2. Install the plugins required by this recipe. Follow [Deployment > Download The Connector Plugins](../locally/deployment.md#download-the-connector-plugins), then keep only the plugins below in `config/plugin_config`:

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

3. If you use Flink or Spark, add the Iceberg dependencies required by your environment, including `hive-exec` and `libfb303` when needed.

4. Choose an empty, writable Iceberg warehouse path. This tutorial uses a local Hadoop catalog at `file:///tmp/seatunnel/iceberg/warehouse-demo`:

```bash
mkdir -p /tmp/seatunnel/iceberg/warehouse-demo
```

5. Create the Kafka topic and produce a few JSON messages before starting the job:

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

## Minimal configuration

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

## Run the job

Save the config as `config/kafka-to-iceberg.conf`, then start SeaTunnel in local mode:

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/kafka-to-iceberg.conf -m local
```

Keep the job running while Kafka messages are being consumed, because this is a streaming pipeline.

## Validation result

1. Verify that Iceberg metadata and data files appear under the warehouse path.
2. Query the Iceberg table with Spark, Trino, or another Iceberg-compatible engine.

```bash
ls /tmp/seatunnel/iceberg/warehouse-demo/lakehouse/orders
spark-sql \
  --conf spark.sql.catalog.seatunnel_demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.seatunnel_demo.type=hadoop \
  --conf spark.sql.catalog.seatunnel_demo.warehouse=file:///tmp/seatunnel/iceberg/warehouse-demo \
  -e "SELECT COUNT(*) FROM seatunnel_demo.lakehouse.orders"
```

If the table can be queried and the row count matches the Kafka messages you produced, the pipeline is working. With the two sample messages above, the count should be `2`.

## Common pitfalls

- JSON messages in Kafka do not match the schema defined in the source block.
- Checkpointing is disabled in a streaming pipeline, which weakens restart and consistency behavior.
- The Iceberg catalog type is correct, but the warehouse path is not writable by the engine process.
- Upsert mode is enabled even though the incoming records do not have stable primary keys.

## Related docs

- [Kafka source](../../connectors/source/Kafka.md)
- [Iceberg sink](../../connectors/sink/Iceberg.md)
