---
sidebar_position: 6
title: Multi-table CDC
---

# Multi-table CDC

Use this recipe when you want one SeaTunnel job to capture changes from multiple upstream tables and route each table to its own downstream table automatically.

## Prerequisites

- Finish [Run your first job](../locally/run-your-first-job.md).
- Install the `connector-cdc-mysql` and `connector-jdbc` plugins.
- Put both the MySQL JDBC driver and the target database JDBC driver into `${SEATUNNEL_HOME}/lib`.
- Make sure the upstream tables have stable primary keys if you want reliable downstream upsert behavior.

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
    username = "postgres"
    password = "password"
    generate_sink_sql = true
    database = "sync_demo"
    table = "public.st_${table_name}"
    primary_keys = ["${primary_key}"]
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

## Validation result

1. Start the job and let the initial snapshot finish.
2. Confirm that multiple target tables are created automatically.
3. Apply inserts or updates on each upstream table and verify they appear in the corresponding downstream table.

```sql
SELECT COUNT(*) FROM public.st_orders;
SELECT COUNT(*) FROM public.st_customers;
SELECT COUNT(*) FROM public.st_products;
```

If each upstream table is routed to its own target table and changes continue to flow, the multi-table CDC pipeline is working.

## Common pitfalls

- The regular expression in `table-pattern` is not escaped correctly. In HOCON, `.` usually needs `\\.` when you mean a literal dot.
- Placeholder-based sink routing is not configured, so multiple source tables are written into one target table accidentally.
- Upstream tables do not have primary keys, but the sink is configured as if upsert semantics were available.
- The downstream naming convention is valid for one database but invalid for another because schema and table placeholders are used differently.

## Related docs

- [MySQL CDC source](../../connectors/source/MySQL-CDC.md)
- [JDBC sink](../../connectors/sink/Jdbc.md)
- [Multi-table synchronization architecture](../../architecture/features/multi-table.md)
