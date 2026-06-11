---
sidebar_position: 1
title: MySQL CDC to Doris
---

# MySQL CDC to Doris

Use this recipe when you want to capture row-level changes from MySQL and keep a Doris table updated continuously.

## Prerequisites

- Finish [Run your first job](../locally/run-your-first-job.md) and make sure local execution works.

1. Prepare the MySQL source table. This recipe relies on a stable primary key so that updates and deletes can be replayed correctly downstream.

```sql
CREATE DATABASE IF NOT EXISTS inventory;

CREATE TABLE IF NOT EXISTS inventory.orders (
  id BIGINT PRIMARY KEY,
  order_status VARCHAR(32),
  amount DECIMAL(10, 2),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO inventory.orders (id, order_status, amount, updated_at) VALUES
  (1001, 'CREATED', 19.99, NOW()),
  (1002, 'CREATED', 29.99, NOW());
```

2. Create the MySQL CDC user and grant the same privileges required by the [MySQL CDC source](../../connectors/source/MySQL-CDC.md):

```sql
CREATE USER IF NOT EXISTS 'st_user_source'@'%' IDENTIFIED BY 'mysqlpw';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'st_user_source'@'%';
FLUSH PRIVILEGES;
```

3. Verify that MySQL binlog is ready for CDC:

```sql
SHOW VARIABLES WHERE variable_name IN ('log_bin', 'binlog_format', 'binlog_row_image');
```

The expected values are `log_bin = ON`, `binlog_format = ROW`, and `binlog_row_image = FULL`.
If they are not set yet, update `my.cnf` and restart MySQL:

```ini
[mysqld]
server-id = 223344
log_bin = mysql-bin
binlog_format = ROW
binlog_row_image = FULL
```

4. Prepare the Doris target database. This recipe keeps `schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"`, so SeaTunnel will create `sync_demo.orders` automatically from the MySQL primary-key metadata on first startup.

```sql
CREATE DATABASE IF NOT EXISTS sync_demo;
```

5. Install the `connector-cdc-mysql` and `connector-doris` plugins, and put the MySQL JDBC driver into `${SEATUNNEL_HOME}/lib` for SeaTunnel Zeta, or into the engine plugin directory for Spark or Flink.

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
    startup.mode = "initial"
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
2. Change the source rows in MySQL:

```sql
INSERT INTO inventory.orders (id, order_status, amount, updated_at)
VALUES (1003, 'CREATED', 39.99, NOW());

UPDATE inventory.orders
SET order_status = 'PAID', updated_at = NOW()
WHERE id = 1001;

DELETE FROM inventory.orders
WHERE id = 1002;
```

3. Query Doris and confirm that the latest state is visible there:

```sql
SELECT COUNT(*) FROM sync_demo.orders;
SELECT id, order_status, amount FROM sync_demo.orders ORDER BY id;
```

You should now see row `1001` with status `PAID`, row `1003` with status `CREATED`, and no remaining row `1002`. If inserts, updates, and deletes from MySQL are reflected in Doris, the pipeline is working.

## Common pitfalls

- MySQL binlog is not enabled or is not using `ROW` format.
- The CDC user is missing replication privileges.
- The `server-id` conflicts with another MySQL replica or another CDC job.
- `sink.label-prefix` is reused across multiple running jobs, which can cause Doris stream load conflicts.
- Delete propagation is enabled, but the Doris table model does not support the expected delete behavior.
- The source table has no stable primary key, so Doris auto-create and downstream upsert behavior are not deterministic.

## Related docs

- [MySQL CDC source](../../connectors/source/MySQL-CDC.md)
- [Doris sink](../../connectors/sink/Doris.md)
- [SeaTunnel Engine quick start](../locally/quick-start-seatunnel-engine.md)
