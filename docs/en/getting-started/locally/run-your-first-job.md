---
sidebar_position: 1
title: Run your first job
---

# Run your first job

This page gives you the shortest path to a successful SeaTunnel run. The example stays fully local, does not require MySQL, Kafka, or object storage, and helps you confirm that your installation, config parsing, and execution engine are all working.

## Step 1: Finish local deployment

Complete [Deployment](deployment.md) first and make sure `bin/seatunnel.sh` is available in your SeaTunnel home directory.

## Step 2: Use a minimal job

Save the following config as `config/v2.batch.config.template` or another local file:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

transform {
  FieldMapper {
    plugin_input = "fake"
    plugin_output = "fake1"
    field_mapper = {
      age = age
      name = new_name
    }
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

## Step 3: Run it in local mode

```shell
cd "apache-seatunnel-${version}"
./bin/seatunnel.sh --config ./config/v2.batch.config.template -m local
```

## Expected validation result

- The process starts successfully without connector loading errors.
- The console prints an `output rowType` line for the mapped fields.
- The console prints 16 rows from `ConsoleSinkWriter`.
- The batch job exits successfully after all rows are written.

If this works, your basic local path is healthy and you can move on to real pipelines.

## Next step

- For the full local walkthrough, continue with [Quick Start With SeaTunnel Engine](quick-start-seatunnel-engine.md).
- For runnable source-to-sink examples, start with these recipes:
  - [MySQL CDC to Doris](../recipes/mysql-cdc-to-doris.md)
  - [JDBC to S3](../recipes/jdbc-to-s3.md)
  - [Kafka to Iceberg](../recipes/kafka-to-iceberg.md)
  - [Http to JDBC](../recipes/http-to-jdbc.md)
  - [File to StarRocks](../recipes/file-to-starrocks.md)
  - [Multi-table CDC](../recipes/multi-table-cdc.md)
