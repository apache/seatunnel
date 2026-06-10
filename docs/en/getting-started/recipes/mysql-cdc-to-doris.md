---
sidebar_position: 1
title: MySQL CDC to Doris
---

# MySQL CDC to Doris

Use this recipe when you want to capture row-level changes from MySQL and keep a Doris table updated continuously.

## Prerequisites

- Finish [Run your first job](../locally/run-your-first-job.md) and make sure local execution works.
- Install the `connector-cdc-mysql` and `connector-doris` plugins.
- Put the MySQL JDBC driver into `${SEATUNNEL_HOME}/lib` for SeaTunnel Zeta, or into the engine plugin directory for Spark or Flink.
- Enable MySQL binlog with `ROW` format and create a CDC user with `SELECT`, `RELOAD`, `SHOW DATABASES`, `REPLICATION SLAVE`, and `REPLICATION CLIENT` permissions.
- Create the target Doris database and a table model that matches your update and delete requirements. If you enable delete propagation, the target table should use a model that supports it.

## Minimal configuration

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    plugin_output = "orders_cdc"
    parallelism = 1
    server-id = 5652
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["inventory.orders"]
    url = "jdbc:mysql://mysql:3306/inventory"
  }
}

sink {
  Doris {
    plugin_input = "orders_cdc"
    fenodes = "doris-fe:8030"
    username = "root"
    password = ""
    database = "sync_demo"
    table = "orders"
    sink.label-prefix = "orders-cdc"
    sink.enable-delete = true
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    doris.config = {
      format = "csv"
      column_separator = ","
    }
  }
}
```

## Validation result

1. Start the job and wait for the initial snapshot to finish.
2. Insert, update, and delete a few rows in MySQL.
3. Query Doris and confirm the latest state is visible there.

```sql
SELECT COUNT(*) FROM sync_demo.orders;
SELECT id, order_status, updated_at FROM sync_demo.orders ORDER BY id;
```

If inserts, updates, and deletes from MySQL are reflected in Doris, the pipeline is working.

## Common pitfalls

- MySQL binlog is not enabled or is not using `ROW` format.
- The CDC user is missing replication privileges.
- `sink.label-prefix` is reused across multiple running jobs, which can cause Doris stream load conflicts.
- Delete propagation is enabled, but the Doris table model does not support the expected delete behavior.
- The source table has no stable primary key, so downstream upsert behavior is not deterministic.

## Related docs

- [MySQL CDC source](../../connectors/source/MySQL-CDC.md)
- [Doris sink](../../connectors/sink/Doris.md)
- [SeaTunnel Engine quick start](../locally/quick-start-seatunnel-engine.md)
