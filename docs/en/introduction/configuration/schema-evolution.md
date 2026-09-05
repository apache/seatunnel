# Schema evolution
Schema Evolution means that the schema of a data table can be changed and the data synchronization task can automatically adapt to the changes of the new table structure without any other operations.

## Supported engines

- Zeta
- Flink

Spark does not support schema evolution or enforce `schema-changes.behavior`. Do not enable
`schema-changes.enabled` for Spark jobs.

## Supported schema change event types

- `ADD COLUMN`
- `DROP COLUMN`
- `RENAME COLUMN`
- `MODIFY COLUMN`

## Supported connectors

### Source
[Mysql-CDC](../../connectors/source/MySQL-CDC.md)
[Oracle-CDC](../../connectors/source/Oracle-CDC.md)

### Sink
[Jdbc-Mysql](../../connectors/sink/Jdbc.md)
[Jdbc-Oracle](../../connectors/sink/Jdbc.md)
[Jdbc-Postgres](../../connectors/sink/Jdbc.md)
[Jdbc-Dameng](../../connectors/sink/Jdbc.md)
[Jdbc-SqlServer](../../connectors/sink/Jdbc.md)
[StarRocks](../../connectors/sink/StarRocks.md)
[Doris](../../connectors/sink/Doris.md)
[Paimon](../../connectors/sink/Paimon.md#schema-evolution)
[Elasticsearch](../../connectors/sink/Elasticsearch.md#schema-evolution)
[Redis](../../connectors/sink/Redis.md#schema-evolution)

Note:  
* The schema evolution is not support the transform at now. The schema evolution of different types of databases（Oracle-CDC -> Jdbc-Mysql）is currently not supported the default value of the column in ddl.

* When you use the Oracle-CDC，you can not use the username named `SYS` or `SYSTEM` to modify the table schema, otherwise the ddl event will be filtered out which can lead to the schema evolution not working.
Otherwise, If your table name start with `ORA_TEMP_` will also has the same problem.

* Earlier versions of `Dameng` databases do not support the change of `Varchar` type fields to `Text` type fields.

## Enable schema evolution
Schema evolution is disabled by default in CDC source. You need configure `schema-changes.enabled = true` which is only supported in CDC to enable it.

## Multi-database and multi-table routing

Schema evolution can work with multi-table jobs as long as each upstream table can be mapped to a
stable physical sink table. SeaTunnel resolves sink placeholders before the connector starts, so
you can route by `${database_name}`, `${schema_name}`, and `${table_name}` as documented in [Sink
Options Placeholders](./sink-options-placeholders.md).

Recommended practices:

- Route tables from different upstream databases to different physical sink tables when you want to
  keep schemas isolated.
- Keep `multi_table_sink_replica` enabled if you need parallel sink writers; schema changes are
  coordinated per rendered physical sink table.
- If you intentionally route multiple upstream tables to the same physical sink table, make sure
  those tables stay schema-compatible and that their keys do not conflict.

### Example: same table name in different source databases -> same table name in different sink databases

```hocon
source {
  MySQL-CDC {
    database-names = ["shop_a", "shop_b"]
    table-names = ["shop_a.products", "shop_b.products"]
    url = "jdbc:mysql://mysql-host:3306"
    schema-changes.enabled = true
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://mysql-host:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"
    generate_sink_sql = true
    database = "${database_name}_sink"
    table = "${table_name}"
    primary_keys = ["id"]
    multi_table_sink_replica = 2
  }
}
```

In this example, `shop_a.products` is written to `shop_a_sink.products`, and `shop_b.products` is
written to `shop_b_sink.products`.

If both source tables later execute DDL such as `ALTER TABLE products ADD COLUMN add_column1
VARCHAR(64), ADD COLUMN add_column2 INT`, SeaTunnel applies the schema change independently to
`shop_a_sink.products` and `shop_b_sink.products`, and each sink table continues to receive only
its own database's data.

### Example: same sink database, different sink tables

```hocon
sink {
  jdbc {
    url = "jdbc:mysql://mysql-host:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"
    generate_sink_sql = true
    database = "ods"
    table = "${database_name}_${table_name}"
    primary_keys = ["id"]
  }
}
```

In this example, `shop_a.products` is written to `ods.shop_a_products`, and `shop_b.products` is
written to `ods.shop_b_products`.

### Example: wildcard capture for multiple databases and tables

```hocon
source {
  MySQL-CDC {
    table-pattern = "sales_.*\\..*"
    url = "jdbc:mysql://mysql-host:3306"
    schema-changes.enabled = true
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://mysql-host:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"
    generate_sink_sql = true
    database = "ods"
    table = "${database_name}_${table_name}"
    primary_keys = ["${primary_key}"]
  }
}
```

## Schema change behavior

CDC sources can configure `schema-changes.behavior` when `schema-changes.enabled = true`.
The default value is `evolve`, so existing jobs that only set `schema-changes.enabled = true` keep the closest existing behavior.
When `schema-changes.enabled = false`, schema change events are not sent downstream and the behavior option does not change the current behavior.
Values are case-insensitive; the lowercase forms below are canonical in configuration examples.

`schema-changes.include` and `schema-changes.exclude` are evaluated first by the CDC
deserializer. A fully excluded event is neither applied to the produced row schema nor passed to
the behavior policy. Therefore, for example, `strict` does not fail for an excluded event.

| Value | Runtime contract |
| --- | --- |
| `strict` | Fail the job as soon as a schema change event is observed, before downstream schema coordination or sink-side schema mutation is attempted. |
| `evolve` | Forward supported schema change events through the normal schema coordination path. Unsupported row-layout changes and sink-side apply failures are fatal. Unsupported comment-only events are logged and dropped on each sink path because they do not affect row encoding. |
| `ignore` | Drop only `ALTER_TABLE_COMMENT` and `ALTER_COLUMN_COMMENT` before downstream schema coordination and sink-side schema evolution. ADD, DROP, RENAME, and MODIFY COLUMN change the runtime row layout and fail instead of being ignored. |

Behavior matrix:

| Case | `strict` | `evolve` | `ignore` |
| --- | --- | --- | --- |
| Source emits supported schema change type | Fail before downstream propagation | Coordinate and apply through the sink | Drop before downstream propagation only if safe to ignore |
| Source emits unsupported schema change type | Fail before downstream propagation | Per sink path, log and drop comment-only events at the Flink policy gate before coordination or at the Zeta sink lifecycle after coordination; otherwise fail before sink-side apply | Drop `ALTER_TABLE_COMMENT` and `ALTER_COLUMN_COMMENT` before coordination; fail for row-layout changes |
| Sink supports schema evolution | Not reached | Apply through `SupportSchemaEvolutionSinkWriter` | Not reached |
| Sink does not support schema evolution | Not reached | Log and drop comment-only events. During the one-release compatibility window, call an explicitly overridden deprecated method, or log and drop for the inherited no-op | Not reached |
| Sink apply throws at runtime | Not reached | Fail the job with the sink apply error | Not reached |

Upgrade note: in `evolve` mode, sink writers should implement
`SupportSchemaEvolutionSinkWriter` to receive and apply schema change events. During the deprecation
window, the Zeta single-table, Zeta multi-table, and both Flink sink paths still invoke a deprecated
`SinkWriter.applySchemaChange` method when the writer explicitly overrides it, and log a migration
warning. For one release, an inherited default no-op also logs a warning and drops the event to
avoid breaking an existing job on upgrade. This fallback will be removed in the next release.
Migrate the sink writer to `SupportSchemaEvolutionSinkWriter`, disable
`schema-changes.enabled`, or exclude event types the sink must not receive. Use
`schema-changes.behavior = ignore` only for comment-only changes.

Policy failures are deterministic. Zeta marks them non-retryable, and Flink wraps them in
`SuppressRestartsException`, so restoring the same checkpoint does not repeatedly replay the same
rejected DDL. Change the behavior to `evolve` with a compatible sink, disable
`schema-changes.enabled`, or adjust the filters before resubmitting the job.

Schema change delivery in `evolve` mode can be repeated after recovery if the external DDL succeeds
but the following checkpoint does not complete. `SupportSchemaEvolutionSinkWriter` implementations
must therefore apply events idempotently. The JDBC implementation checks the current sink schema
before replaying ADD, DROP, and RENAME COLUMN operations; other sink implementations must provide
the equivalent guarantee for the event types they advertise.

## Examples

### Mysql-CDC -> Jdbc-Mysql
```
env {
  # You can set engine configuration here
  parallelism = 5
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  read_limit.bytes_per_second=7000000
  read_limit.rows_per_second=400
}

source {
  MySQL-CDC {
    server-id = 5652-5657
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    
    schema-changes.enabled = true
    schema-changes.behavior = evolve
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "st_user_sink"
    password = "mysqlpw"
    generate_sink_sql = true
    database = shop
    table = mysql_cdc_e2e_sink_table_with_schema_change_exactly_once
    primary_keys = ["id"]
    is_exactly_once = true
    xa_data_source_class_name = "com.mysql.cj.jdbc.MysqlXADataSource"
  }
}
```

### Oracle-cdc -> Jdbc-Oracle
```
env {
  # You can set engine configuration here
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  Oracle-CDC {
    plugin_output = "customers"
    username = "dbzuser"
    password = "dbz"
    database-names = ["ORCLCDB"]
    schema-names = ["DEBEZIUM"]
    table-names = ["ORCLCDB.DEBEZIUM.FULL_TYPES"]
    url = "jdbc:oracle:thin:@oracle-host:1521/ORCLCDB"
    source.reader.close.timeout = 120000
    connection.pool.size = 1
    
    schema-changes.enabled = true
  }
}

sink {
    Jdbc {
      plugin_input = "customers"
      driver = "oracle.jdbc.driver.OracleDriver"
      url = "jdbc:oracle:thin:@oracle-host:1521/ORCLCDB"
      user = "dbzuser"
      password = "dbz"
      generate_sink_sql = true
      database = "ORCLCDB"
      table = "DEBEZIUM.FULL_TYPES_SINK"
      batch_size = 1
      primary_keys = ["ID"]
      connection.pool.size = 1
    }
}
```

### Oracle-cdc -> Jdbc-Mysql
```
env {
  # You can set engine configuration here
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  Oracle-CDC {
    plugin_output = "customers"
    username = "dbzuser"
    password = "dbz"
    database-names = ["ORCLCDB"]
    schema-names = ["DEBEZIUM"]
    table-names = ["ORCLCDB.DEBEZIUM.FULL_TYPES"]
    url = "jdbc:oracle:thin:@oracle-host:1521/ORCLCDB"
    source.reader.close.timeout = 120000
    connection.pool.size = 1
    
    schema-changes.enabled = true
  }
}

sink {
  jdbc {
    plugin_input = "customers"
    url = "jdbc:mysql://oracle-host:3306/oracle_sink"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "st_user_sink"
    password = "mysqlpw"
    generate_sink_sql = true
    # You need to configure both database and table
    database = oracle_sink
    table = oracle_cdc_2_mysql_sink_table
    primary_keys = ["ID"]
  }
}
```

### Mysql-cdc -> StarRocks
```
env {
  # You can set engine configuration here
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    
    schema-changes.enabled = true
  }
}

sink {
  StarRocks {
    nodeUrls = ["starrocks_cdc_e2e:8030"]
    username = "root"
    password = ""
    database = "shop"
    table = "${table_name}"
    base-url = "jdbc:mysql://starrocks_cdc_e2e:9030/shop"
    max_retries = 3
    enable_upsert_delete = true
    schema_save_mode="RECREATE_SCHEMA"
    data_save_mode="DROP_DATA"
    save_mode_create_template = """
    CREATE TABLE IF NOT EXISTS shop.`${table_name}` (
        ${rowtype_primary_key},
        ${rowtype_fields}
        ) ENGINE=OLAP
        PRIMARY KEY (${rowtype_primary_key})
        DISTRIBUTED BY HASH (${rowtype_primary_key})
        PROPERTIES (
                "replication_num" = "1",
                "in_memory" = "false",
                "enable_persistent_index" = "true",
                "replicated_storage" = "true",
                "compression" = "LZ4"
          )
    """
  }
}
```
### Mysql-CDC -> Doris
```
env {
  # You can set engine configuration here
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    server-id = 5652-5657
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    schema-changes.enabled = true
  }
}

sink {
  Doris {
    fenodes = "doris_e2e:8030"
    username = "root"
    password = ""
    database = "shop"
    table = "products"
    sink.label-prefix = "test-cdc"
    sink.enable-2pc = "true"
    sink.enable-delete = "true"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}
```

> **Note (schema evolution + 2PC):** With `sink.enable-2pc = "true"`, Doris schema evolution only supports `format = "json"` because JSON loads match columns by name. Positional formats such as CSV are rejected at runtime for schema evolution with 2PC enabled. Use `format = "json"` or set `sink.enable-2pc = "false"` so the sink can flush buffered rows before applying the DDL.

### Mysql-CDC -> Jdbc-Postgres
```hocon
env {
  # You can set engine configuration here
  parallelism = 5
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  read_limit.bytes_per_second=7000000
  read_limit.rows_per_second=400
}

source {
  MySQL-CDC {
    server-id = 5652-5657
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"

    schema-changes.enabled = true
  }
}

sink {
  jdbc {
    url = "jdbc:postgresql://postgresql:5432/shop"
    driver = "org.postgresql.Driver"
    user = "postgres"
    password = "postgres"
    generate_sink_sql = true
    database = shop
    table = "public.sink_table_with_schema_change"
    primary_keys = ["id"]

    # Validate ddl update for sink writer multi replica
    multi_table_sink_replica = 2
  }
}
```

### Mysql-CDC -> Jdbc-Dameng
```hocon
env {
  # You can set engine configuration here
  parallelism = 5
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  read_limit.bytes_per_second=7000000
  read_limit.rows_per_second=400
}

source {
  MySQL-CDC {
    server-id = 5652-5657
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"

    schema-changes.enabled = true
  }
}

sink {
  jdbc {
    url = "jdbc:dm://e2e_dmdb:5236"
    driver = "dm.jdbc.driver.DmDriver"
    connection_check_timeout_sec = 1000
    user = "SYSDBA"
    password = "SYSDBA"
    generate_sink_sql = true
    database = "DAMENG"
    table = "SYSDBA.sink_table_with_schema_change"
    primary_keys = ["id"]

    # Validate ddl update for sink writer multi replica
    multi_table_sink_replica = 2
  }
}
```

### Mysql-CDC -> Jdbc-SqlServer
```hocon
env {
  # You can set engine configuration here
  parallelism = 5
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  read_limit.bytes_per_second=7000000
  read_limit.rows_per_second=400
}

source {
  MySQL-CDC {
    server-id = 5652-5657
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"

    schema-changes.enabled = true
  }
}

sink {
  jdbc {
    url = "jdbc:sqlserver://e2e_sqlserver:1433"
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    user = "sa"
    password = "paanssy1234$"
    generate_sink_sql = true
    database = master
    table = "dbo.sink_table_with_schema_change"
    primary_keys = ["id"]

    # Validate ddl update for sink writer multi replica
    multi_table_sink_replica = 2
  }
}
```
