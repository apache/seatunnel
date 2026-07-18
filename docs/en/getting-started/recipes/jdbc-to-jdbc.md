---
sidebar_position: 3
title: JDBC to JDBC
---

# JDBC to JDBC

Use this recipe when you need a batch migration between relational databases and want to filter or reshape rows before writing them. The example reads orders from MySQL, keeps only paid orders, normalizes customer names, fixes the amount scale, fills a source-system field, and writes the result to PostgreSQL.

## Prerequisites

1. Finish [Run your first job](../locally/run-your-first-job.md).

2. Install the JDBC connector. Follow [Deployment > Download The Connector Plugins](../locally/deployment.md#download-the-connector-plugins), then keep the plugin below in `config/plugin_config`:

```plugin_config
--seatunnel-connectors--
connector-jdbc
--end--
```

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
ls connectors | rg 'connector-jdbc'
```

The `Sql` transform is included in the SeaTunnel distribution and does not require another connector entry.

3. Put both database drivers into `${SEATUNNEL_HOME}/lib`, then confirm that SeaTunnel can see them:

```bash
ls "${SEATUNNEL_HOME}/lib" | rg 'mysql-connector|postgresql'
```

4. Create the MySQL source table and insert three deterministic rows:

```sql
CREATE DATABASE IF NOT EXISTS source_db;

CREATE TABLE IF NOT EXISTS source_db.orders (
  id BIGINT PRIMARY KEY,
  customer_name VARCHAR(100) NOT NULL,
  amount DECIMAL(16, 4) NOT NULL,
  status VARCHAR(20) NOT NULL
);

TRUNCATE TABLE source_db.orders;

INSERT INTO source_db.orders (id, customer_name, amount, status) VALUES
  (1001, 'alice chen', 120.5000, 'PAID'),
  (1002, 'bob li', 80.0000, 'CREATED'),
  (1003, 'carol wu', 42.0000, 'PAID');
```

5. Create the PostgreSQL target database and table. The sink user needs `INSERT` permission on this table.

```sql
CREATE USER test WITH PASSWORD 'test';
CREATE DATABASE target_db OWNER test;
```

Reconnect to `target_db` as `test`, then run:

```sql
CREATE TABLE IF NOT EXISTS public.paid_orders (
  id BIGINT PRIMARY KEY,
  customer_name VARCHAR(100) NOT NULL,
  amount DECIMAL(12, 2) NOT NULL,
  source_system VARCHAR(20) NOT NULL
);

TRUNCATE TABLE public.paid_orders;
```

If the user or database already exists, reuse it instead of running the corresponding `CREATE` statement.

## Complete configuration

The `plugin_output` and `plugin_input` values connect the three stages. The transformed column order also matches the four placeholders in the sink `query`; keep those two orders aligned when adapting the example.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    plugin_output = "mysql_orders"
    driver = "com.mysql.cj.jdbc.Driver"
    url = "jdbc:mysql://mysql:3306/source_db?useSSL=false&allowPublicKeyRetrieval=true"
    username = "root"
    password = "password"
    query = "SELECT id, customer_name, amount, status FROM orders"
  }
}

transform {
  Sql {
    plugin_input = "mysql_orders"
    plugin_output = "paid_orders"
    query = """
      SELECT
        id,
        UPPER(customer_name) AS customer_name,
        CAST(amount AS DECIMAL(12, 2)) AS amount,
        'MYSQL' AS source_system
      FROM dual
      WHERE status = 'PAID'
    """
  }
}

sink {
  Jdbc {
    plugin_input = "paid_orders"
    driver = "org.postgresql.Driver"
    url = "jdbc:postgresql://postgresql:5432/target_db"
    username = "test"
    password = "test"
    query = "INSERT INTO public.paid_orders (id, customer_name, amount, source_system) VALUES (?, ?, ?, ?)"
  }
}
```

`dual` is the virtual input table used by the default SeaTunnel SQL transform engine. It does not refer to a MySQL or PostgreSQL table.

## Run the job

Save the configuration as `config/jdbc-to-jdbc.conf`, replace the hostnames and credentials with values for your environment, and run SeaTunnel in local mode:

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/jdbc-to-jdbc.conf -m local
```

## Verify the result

Query the PostgreSQL target after the job finishes:

```sql
SELECT id, customer_name, amount, source_system
FROM public.paid_orders
ORDER BY id;
```

Expected result:

| id | customer_name | amount | source_system |
| --- | --- | ---: | --- |
| 1001 | ALICE CHEN | 120.50 | MYSQL |
| 1003 | CAROL WU | 42.00 | MYSQL |

This result proves each transformation independently: order `1002` was filtered out because it was not paid, names were uppercased, amounts have scale 2, and the constant `source_system` field was added. The repository's companion Docker E2E test starts MySQL and PostgreSQL containers, runs this pipeline, and asserts these exact target rows.

## Common pitfalls

- The MySQL or PostgreSQL driver is missing from `${SEATUNNEL_HOME}/lib`, or its version is incompatible with the database.
- A Docker hostname such as `mysql` or `postgresql` is copied into a non-Docker environment without being replaced by a resolvable host.
- The selected field order changes, but the columns in the sink `INSERT` statement do not change with it.
- The target table is not empty and a second run conflicts with its primary key. Truncate the table for a repeatable tutorial run, or choose an upsert strategy for production.
- A source `DECIMAL` value exceeds the precision or scale declared by `DECIMAL(12, 2)`. Choose a target type that fits the real data before migrating it.
- The source changes while a batch job is running. Use a CDC source instead when changes must be captured continuously.

## Related docs

- [JDBC source](../../connectors/source/Jdbc.md)
- [SQL transform](../../transforms/sql.md)
- [JDBC sink](../../connectors/sink/Jdbc.md)
- [Multi-Table CDC](./multi-table-cdc.md)
