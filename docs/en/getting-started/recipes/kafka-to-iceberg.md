---
sidebar_position: 3
title: Kafka to Iceberg
---

# Kafka to Iceberg

Use this recipe when you want to land streaming events from Kafka into an Iceberg table for downstream analytics.

## Prerequisites

- Finish [Run your first job](../locally/run-your-first-job.md).
- Install the `connector-kafka` and `connector-iceberg` plugins.
- Prepare a Kafka topic with JSON messages.
- If you use Flink or Spark, add the Iceberg dependencies required by your environment, including `hive-exec` and `libfb303` when needed.
- Decide which Iceberg catalog to use. The example below uses a local Hadoop catalog because it is the simplest path for validation.

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

## Validation result

1. Produce a few JSON records into the Kafka topic.
2. Start the SeaTunnel job.
3. Verify that Iceberg metadata and data files appear under the warehouse path.
4. Query the Iceberg table with Spark, Trino, or another Iceberg-compatible engine.

```bash
ls /tmp/seatunnel/iceberg/warehouse/lakehouse/orders
spark-sql \
  --conf spark.sql.catalog.seatunnel_demo=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.seatunnel_demo.type=hadoop \
  --conf spark.sql.catalog.seatunnel_demo.warehouse=file:///tmp/seatunnel/iceberg/warehouse \
  -e "SELECT COUNT(*) FROM seatunnel_demo.lakehouse.orders"
```

If the table can be queried and the row count matches the Kafka messages you produced, the pipeline is working.

## Common pitfalls

- JSON messages in Kafka do not match the schema defined in the source block.
- Checkpointing is disabled in a streaming pipeline, which weakens restart and consistency behavior.
- The Iceberg catalog type is correct, but the warehouse path is not writable by the engine process.
- Upsert mode is enabled even though the incoming records do not have stable primary keys.

## Related docs

- [Kafka source](../../connectors/source/Kafka.md)
- [Iceberg sink](../../connectors/sink/Iceberg.md)
