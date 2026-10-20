---
title: PostgreSQL CDC to Iceberg with field shaping and upserts
---

# PostgreSQL CDC to Iceberg with field shaping and upserts

This recipe captures an initial PostgreSQL snapshot and subsequent changes, cleans and enriches each row, and maintains the current state in an Iceberg table. The example uses PostgreSQL 14 with `pgoutput` and covers snapshot, update, insert, and delete events.

The pipeline is:

- `Postgres-CDC` reads `sales.inventory.customer_orders` through PostgreSQL logical replication.
- `Sql` trims customer names, normalizes status values, and fills `sync_source`.
- `Iceberg` uses `id` as its identifier field and applies CDC events in upsert mode.

## Prerequisites

1. Finish [Run your first job](../locally/run-your-first-job.md) with the Zeta engine.

2. Install the required connectors:

```plugin_config
--seatunnel-connectors--
connector-cdc-postgres
connector-iceberg
--end--
```

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
ls connectors | grep -E 'connector-(cdc-postgres|iceberg)'
```

3. Put a compatible PostgreSQL JDBC driver in `${SEATUNNEL_HOME}/lib` for Zeta:

```bash
ls "${SEATUNNEL_HOME}/lib" | grep 'postgresql-'
```

4. Enable logical replication on the PostgreSQL primary and restart PostgreSQL after changing `wal_level`:

```conf
wal_level = logical
max_replication_slots = 10
max_wal_senders = 10
```

Confirm the effective settings:

```sql
SHOW wal_level;
SHOW max_replication_slots;
SHOW max_wal_senders;
```

5. As a PostgreSQL administrator, create the source database and a dedicated CDC user. Replace the database, user, and password as needed. If either object already exists, skip its `CREATE` statement and reuse it:

```sql
CREATE DATABASE sales;
```

PostgreSQL requires `CREATE DATABASE` to run outside a transaction. After it completes, run:

```sql
CREATE ROLE seatunnel_cdc WITH REPLICATION LOGIN PASSWORD 'change_me';
GRANT CONNECT ON DATABASE sales TO seatunnel_cdc;
```

Add a matching rule to `pg_hba.conf`. Replace the example CIDR with the network used by your SeaTunnel nodes:

```conf
host  sales  seatunnel_cdc  192.0.2.0/24  scram-sha-256
```

Logical replication connects to the actual database, so this rule names `sales` rather than the `replication` keyword used for physical replication. Reload the PostgreSQL configuration after saving the file:

```sql
SELECT pg_reload_conf();
```

6. Choose an empty Iceberg warehouse path writable by every SeaTunnel worker. A local `file://` warehouse is suitable only when all workers see the same filesystem. Use HDFS, S3, or another shared catalog storage for a distributed cluster.

## Prepare the source data

Connect to the `sales` database as an administrator, create the source schema and table in PostgreSQL 14, then grant the read-only CDC user access:

```sql
CREATE SCHEMA inventory;

CREATE TABLE inventory.customer_orders (
  id BIGINT PRIMARY KEY,
  customer_name VARCHAR(64) NOT NULL,
  amount NUMERIC(10, 2) NOT NULL,
  status VARCHAR(16) NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

ALTER TABLE inventory.customer_orders REPLICA IDENTITY FULL;

INSERT INTO inventory.customer_orders VALUES
  (1001, ' Alice Zhang ', 120.50, 'pending', '2026-07-18 09:00:00'),
  (1002, 'Bob Li', 80.00, 'paid', '2026-07-18 09:05:00');

GRANT USAGE ON SCHEMA inventory TO seatunnel_cdc;
GRANT SELECT ON TABLE inventory.customer_orders TO seatunnel_cdc;

CREATE PUBLICATION seatunnel_sales_orders_pub
FOR TABLE inventory.customer_orders;
```

The administrator creates the publication so that the CDC user can remain read-only. The job configuration below reuses this publication instead of trying to create one for every table in the database.

After the initial snapshot reaches Iceberg, run the following changes with the source application's write account or an administrator account, not the read-only CDC account:

```sql
UPDATE inventory.customer_orders
SET amount = 150.75, status = 'paid', updated_at = '2026-07-18 10:00:00'
WHERE id = 1001;

INSERT INTO inventory.customer_orders VALUES
  (1003, ' Carol Wang ', 42.00, 'pending', '2026-07-18 10:05:00');

DELETE FROM inventory.customer_orders WHERE id = 1002;
```

## Complete job configuration

The following HOCON config implements the complete pipeline. Replace the example hostname, credentials, slot name, and warehouse path for your environment. Each concurrently running CDC job on the same PostgreSQL instance must use a distinct `slot.name`. Keep `publication.name` aligned with the publication created above.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 3000
}

source {
  Postgres-CDC {
    plugin_output = "postgres_orders_raw"
    url = "jdbc:postgresql://postgresql.example.com:5432/sales"
    username = "seatunnel_cdc"
    password = "change_me"
    database-names = ["sales"]
    schema-names = ["inventory"]
    table-names = ["sales.inventory.customer_orders"]
    startup.mode = "initial"
    decoding.plugin.name = "pgoutput"
    slot.name = ${slot_name}
    debezium = {
      "publication.name" = "seatunnel_sales_orders_pub"
      "publication.autocreate.mode" = "disabled"
    }
  }
}

transform {
  Sql {
    plugin_input = "postgres_orders_raw"
    plugin_output = "iceberg_customer_orders"
    query = "select id, trim(customer_name) as customer_name, amount, upper(status) as status_name, updated_at, 'postgresql_cdc' as sync_source from dual"
  }
}

sink {
  Iceberg {
    plugin_input = "iceberg_customer_orders"
    catalog_name = "recipe_catalog"
    iceberg.catalog.config = {
      "type" = "hadoop"
      "warehouse" = ${warehouse}
    }
    namespace = "sales_analytics"
    table = "customer_orders"
    iceberg.table.primary-keys = "id"
    iceberg.table.upsert-mode-enabled = true
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

Save the config as `${SEATUNNEL_HOME}/config/postgresql-cdc-to-iceberg.conf`, replace `${slot_name}` and `${warehouse}` with quoted literal values, and run:

```bash
cd "${SEATUNNEL_HOME}"
./bin/seatunnel.sh --config ./config/postgresql-cdc-to-iceberg.conf -m local
```

For example:

```hocon
slot.name = "seatunnel_sales_orders"
"warehouse" = "file:///tmp/seatunnel/iceberg/postgres-cdc-recipe/"
```

## Verified result

After the incremental SQL is committed and the next Iceberg checkpoint completes, `sales_analytics.customer_orders` contains exactly these rows:

| id | customer_name | amount | status_name | sync_source |
| --- | --- | ---: | --- | --- |
| 1001 | Alice Zhang | 150.75 | PAID | postgresql_cdc |
| 1003 | Carol Wang | 42.00 | PENDING | postgresql_cdc |

Query the Iceberg table and verify the exact primary-key set and transformed fields shown above. Row `1002` must no longer exist.

## Operational checks

- Monitor `pg_replication_slots`. A stopped consumer can retain WAL indefinitely.
- Drop a slot only after its CDC job is permanently decommissioned; never share a slot between active jobs.
- Keep `iceberg.table.primary-keys` aligned with a stable source key. Upsert mode requires this option explicitly.
- Ensure checkpoints continue to complete. Iceberg changes become visible after a successful commit.
- Do not use a node-local warehouse path in a multi-worker cluster unless the path is backed by shared storage.

## Troubleshooting

- `wal_level` is not `logical`, or PostgreSQL was not restarted after the setting changed.
- The CDC account lacks `REPLICATION`, `CONNECT`, schema `USAGE`, or table `SELECT` privileges.
- Another job is already using the configured replication slot.
- The source table does not use `REPLICA IDENTITY FULL` while the default safety check is enabled.
- The Iceberg warehouse is not writable or is not visible from every SeaTunnel worker.
- Upsert mode is enabled without an explicit `iceberg.table.primary-keys` value.

## Related documentation

- [PostgreSQL CDC source](../../connectors/source/PostgreSQL-CDC.md)
- [Iceberg sink](../../connectors/sink/Iceberg.md)
- [Sql transform](../../transforms/sql.md)
