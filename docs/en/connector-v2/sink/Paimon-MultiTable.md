# Paimon Multi-Table Sink

## Description

The Paimon connector supports writing to multiple tables in a single sink configuration. This feature allows you to write data to multiple Paimon tables simultaneously, which is useful for scenarios like data distribution, ETL processes, and data replication.

## Key Features

- **Multi-table writing**: Write to multiple Paimon tables in a single sink
- **Individual table configuration**: Each table can have its own schema, save modes, and table properties
- **Automatic table routing**: Data is automatically routed to the correct table based on the `tableId`
- **Flexible configuration**: Different tables can have different configurations

## Options

| Name       | Type   | Required | Default | Description                                                                 |
|------------|--------|----------|---------|-----------------------------------------------------------------------------|
| table_list | array  | No       | -       | List of tables to be written. Use this instead of `table` for multi-table writing |

### table_list Configuration

Each table in the `table_list` array supports the following options:

| Name              | Type   | Required | Default                              | Description                           |
|-------------------|--------|----------|--------------------------------------|---------------------------------------|
| database          | string | Yes      | -                                    | The database name                     |
| table             | string | Yes      | -                                    | The table name                        |
| schema_save_mode  | enum   | No       | CREATE_SCHEMA_WHEN_NOT_EXIST         | Schema save mode for the table        |
| data_save_mode    | enum   | No       | APPEND_DATA                          | Data save mode for the table          |
| paimon.table.primary-keys | string | No | -                              | Primary keys for the table            |
| paimon.table.partition-keys | string | No | -                            | Partition keys for the table          |
| paimon.table.write-props | map | No | {}                                | Write properties for the table        |
| schema            | object | No       | -                                    | Schema definition for the table       |

## Examples

### Example 1: Basic Multi-table Writing

```hocon
sink {
  Paimon {
    warehouse = "file:///tmp/paimon"
    table_list = [
      {
        database = "test_db"
        table = "users"
        schema = {
          fields {
            id = bigint
            name = string
            email = string
          }
          primaryKey {
            name = "id"
            columnNames = [id]
          }
        }
      },
      {
        database = "test_db"
        table = "orders"
        schema = {
          fields {
            order_id = bigint
            user_id = bigint
            amount = "decimal(10,2)"
            created_at = timestamp
          }
          primaryKey {
            name = "order_id"
            columnNames = [order_id]
          }
        }
      }
    ]
  }
}
```

### Example 2: Multi-table with Different Configurations

```hocon
sink {
  Paimon {
    warehouse = "file:///tmp/paimon"
    table_list = [
      {
        database = "test_db"
        table = "users"
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode = "APPEND_DATA"
        "paimon.table.primary-keys" = "id"
        "paimon.table.write-props" = {
          "bucket" = "4"
          "file.format" = "parquet"
        }
        schema = {
          fields {
            id = bigint
            name = string
            status = string
          }
        }
      },
      {
        database = "test_db"
        table = "logs"
        schema_save_mode = "RECREATE_SCHEMA"
        data_save_mode = "DROP_DATA"
        "paimon.table.partition-keys" = "dt"
        "paimon.table.write-props" = {
          "bucket" = "8"
          "file.format" = "orc"
        }
        schema = {
          fields {
            log_id = bigint
            message = string
            dt = date
          }
        }
      }
    ]
  }
}
```

### Example 3: Single Table (Backward Compatibility)

```hocon
sink {
  Paimon {
    warehouse = "file:///tmp/paimon"
    database = "test_db"
    table = "users"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
    schema = {
      fields {
        id = bigint
        name = string
        email = string
      }
      primaryKey {
        name = "id"
        columnNames = [id]
      }
    }
  }
}
```

## Complete Example with Source

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    tables_configs = [
      {
        schema = {
          table = "test_db.users"
          fields {
            id = bigint
            name = string
            email = string
          }
        }
        row.num = 100
      },
      {
        schema = {
          table = "test_db.orders"
          fields {
            order_id = bigint
            user_id = bigint
            amount = "decimal(10,2)"
          }
        }
        row.num = 50
      }
    ]
  }
}

sink {
  Paimon {
    warehouse = "file:///tmp/paimon"
    table_list = [
      {
        database = "test_db"
        table = "users"
        schema = {
          fields {
            id = bigint
            name = string
            email = string
          }
          primaryKey {
            name = "id"
            columnNames = [id]
          }
        }
      },
      {
        database = "test_db"
        table = "orders"
        schema = {
          fields {
            order_id = bigint
            user_id = bigint
            amount = "decimal(10,2)"
          }
          primaryKey {
            name = "order_id"
            columnNames = [order_id]
          }
        }
      }
    ]
  }
}
```

## Notes

1. **Exclusive Options**: You cannot use both `table` and `table_list` in the same configuration. Use `table_list` for multi-table writing and `table` for single table writing.

2. **Table Routing**: Data is automatically routed to the correct table based on the `tableId` field in each `SeaTunnelRow`. Make sure your source data has the correct `tableId` set.

3. **Schema Consistency**: Each table can have its own schema definition, but make sure the source data matches the expected schema for each table.

4. **Performance**: Multi-table writing creates separate writers for each table, so consider the performance implications when writing to many tables simultaneously.

5. **Backward Compatibility**: Existing single-table configurations will continue to work without any changes.

6. **Save Modes**: Each table can have its own `schema_save_mode` and `data_save_mode`, allowing fine-grained control over how each table is created and populated.

7. **Table Properties**: Each table can have its own Paimon-specific properties like primary keys, partition keys, and write properties.
