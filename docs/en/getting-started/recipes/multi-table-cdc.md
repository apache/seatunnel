---
sidebar_position: 6
title: Multi-table CDC
---

# Multi-table CDC

Use this recipe when you want one SeaTunnel job to capture changes from multiple upstream tables and route each table to its own downstream table automatically.

## Prerequisites

1. Finish [Run your first job](../locally/run-your-first-job.md).

2. Install the plugins required by this recipe. Follow [Deployment > Download The Connector Plugins](../locally/deployment.md#download-the-connector-plugins), then keep only the plugins below in `config/plugin_config`:

```plugin_config
--seatunnel-connectors--
connector-cdc-mysql
connector-jdbc
--end--
```

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
ls connectors | rg 'connector-(cdc-mysql|jdbc)'
```

3. If you use SeaTunnel Zeta, place both the MySQL JDBC driver and the PostgreSQL JDBC driver into `${SEATUNNEL_HOME}/lib`, then confirm they are visible:

```bash
ls "${SEATUNNEL_HOME}/lib" | rg 'mysql-connector|postgresql'
```

4. Prepare the MySQL source tables. Each upstream table should have a stable primary key because this recipe routes CDC changes to downstream upsert tables automatically.

```sql
CREATE DATABASE IF NOT EXISTS inventory;

CREATE TABLE IF NOT EXISTS inventory.orders (
  id BIGINT PRIMARY KEY,
  order_status VARCHAR(32),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS inventory.customers (
  id BIGINT PRIMARY KEY,
  customer_name VARCHAR(64),
  city VARCHAR(64),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS inventory.products (
  id BIGINT PRIMARY KEY,
  product_name VARCHAR(64),
  unit_price DECIMAL(10, 2),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO inventory.orders (id, order_status, updated_at) VALUES
  (2001, 'CREATED', NOW());

INSERT INTO inventory.customers (id, customer_name, city, updated_at) VALUES
  (3001, 'Alice', 'Shanghai', NOW());

INSERT INTO inventory.products (id, product_name, unit_price, updated_at) VALUES
  (4001, 'Keyboard', 99.00, NOW());
```

5. Create the MySQL CDC user and grant the required privileges:

```sql
CREATE USER IF NOT EXISTS 'st_user_source'@'%' IDENTIFIED BY 'mysqlpw';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
ON *.* TO 'st_user_source'@'%';
FLUSH PRIVILEGES;
```

6. Verify that MySQL binlog is ready:

```sql
SHOW VARIABLES WHERE variable_name IN ('log_bin', 'binlog_format', 'binlog_row_image');
```

The expected values are `log_bin = ON`, `binlog_format = ROW`, and `binlog_row_image = FULL`.

7. Prepare the PostgreSQL target database and grant the sink user permission to create tables in `public`:

```sql
CREATE USER st_user_sink WITH PASSWORD 'pgpw';
CREATE DATABASE sync_demo;
GRANT ALL PRIVILEGES ON DATABASE sync_demo TO st_user_sink;
```

Reconnect to `sync_demo`, then run:

```sql
GRANT USAGE, CREATE ON SCHEMA public TO st_user_sink;
```

This recipe uses `generate_sink_sql = true`, so SeaTunnel will create tables such as `public.st_orders` and `public.st_customers` automatically on the first run.

## Minimal configuration

This example reads multiple MySQL tables through one `table-pattern` and writes them to PostgreSQL tables named `st_<upstream_table_name>`.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    plugin_output = "mysql_multi"
    startup.mode = "initial"
    server-id = 5652
    username = "st_user_source"
    password = "mysqlpw"
    database-pattern = "inventory"
    table-pattern = "inventory\\.(orders|customers|products)"
    url = "jdbc:mysql://mysql:3306/inventory"
  }
}

sink {
  Jdbc {
    plugin_input = "mysql_multi"
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://postgresql:5432/sync_demo"
    username = "st_user_sink"
    password = "pgpw"
    generate_sink_sql = true
    database = "sync_demo"
    table = "public.st_${table_name}"
    primary_keys = ["${primary_key}"]
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

## Run the job

Save the config as `config/multi-table-cdc.conf`, then start SeaTunnel in local mode:

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/multi-table-cdc.conf -m local
```

Keep the job running while you verify new changes from MySQL, because this is a streaming CDC pipeline.

## Validation result

1. Start the job and let the initial snapshot finish.
2. Confirm that SeaTunnel created multiple target tables in PostgreSQL:

```sql
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public' AND table_name LIKE 'st_%'
ORDER BY table_name;
```

3. Apply changes on each MySQL source table:

```sql
INSERT INTO inventory.orders (id, order_status, updated_at)
VALUES (2002, 'PAID', NOW());

UPDATE inventory.customers
SET city = 'Hangzhou', updated_at = NOW()
WHERE id = 3001;

INSERT INTO inventory.products (id, product_name, unit_price, updated_at)
VALUES (4002, 'Mouse', 59.00, NOW());
```

4. Verify that each downstream table received only its own source data:

```sql
SELECT id, order_status FROM public.st_orders ORDER BY id;
SELECT id, customer_name, city FROM public.st_customers ORDER BY id;
SELECT id, product_name, unit_price FROM public.st_products ORDER BY id;
```

If each upstream table is routed to its own target table and changes continue to flow, the multi-table CDC pipeline is working.

## Common pitfalls

- The regular expression in `table-pattern` is not escaped correctly. In HOCON, `.` usually needs `\\.` when you mean a literal dot.
- MySQL binlog or CDC user privileges are incomplete, so the job can read the snapshot but cannot continue reading incremental changes.
- Placeholder-based sink routing is not configured, so multiple source tables are written into one target table accidentally.
- The PostgreSQL sink user can connect to the database but does not have `CREATE` permission on schema `public`.
- Upstream tables do not have primary keys, but the sink is configured as if upsert semantics were available.
- The downstream naming convention is valid for one database but invalid for another because schema and table placeholders are used differently.

## Related docs

- [MySQL CDC source](../../connectors/source/MySQL-CDC.md)
- [JDBC sink](../../connectors/sink/Jdbc.md)
- [Multi-table synchronization architecture](../../architecture/features/multi-table.md)
