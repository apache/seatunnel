# Paimon Multi-Table Source

## Description

The Paimon connector supports reading from multiple tables in a single source configuration. This feature allows you to read data from multiple Paimon tables simultaneously, which is useful for scenarios like data synchronization, ETL processes, and data migration.

## Key Features

- **Multi-table reading**: Read from multiple Paimon tables in a single source
- **Individual table configuration**: Each table can have its own query and schema configuration
- **Automatic table identification**: Each row is automatically tagged with its source table ID
- **Flexible schema support**: Different tables can have different schemas

## Options

| Name       | Type   | Required | Default | Description                                                                 |
|------------|--------|----------|---------|-----------------------------------------------------------------------------|
| table_list | array  | No       | -       | List of tables to be read. Use this instead of `table` for multi-table reading |

### table_list Configuration

Each table in the `table_list` array supports the following options:

| Name     | Type   | Required | Default | Description                           |
|----------|--------|----------|---------|---------------------------------------|
| database | string | Yes      | -       | The database name                     |
| table    | string | Yes      | -       | The table name                        |
| query    | string | No       | -       | SQL query for filtering and projection |
| schema   | object | No       | -       | Schema definition for the table       |

## Examples

### Example 1: Basic Multi-table Reading

```hocon
source {
  Paimon {
    warehouse = "file:///tmp/paimon"
    catalog_name = "paimon_catalog"
    catalog_type = "FILESYSTEM"
    table_list = [
      {
        database = "test_db"
        table = "users"
        schema = {
          fields {
            id = BIGINT
            name = STRING
            email = STRING
          }
        }
      },
      {
        database = "test_db"
        table = "orders"
        schema = {
          fields {
            order_id = BIGINT
            user_id = BIGINT
            amount = DECIMAL(10,2)
            created_at = TIMESTAMP
          }
        }
      }
    ]
  }
}
```

### Example 2: Multi-table with Queries

```hocon
source {
  Paimon {
    warehouse = "file:///tmp/paimon"
    catalog_name = "paimon_catalog"
    catalog_type = "FILESYSTEM"
    table_list = [
      {
        database = "test_db"
        table = "users"
        query = "SELECT id, name FROM users WHERE status = 'active'"
        schema = {
          fields {
            id = BIGINT
            name = STRING
          }
        }
      },
      {
        database = "test_db"
        table = "orders"
        query = "SELECT * FROM orders WHERE amount > 100"
        schema = {
          fields {
            order_id = BIGINT
            user_id = BIGINT
            amount = DECIMAL(10,2)
            created_at = TIMESTAMP
          }
        }
      }
    ]
  }
}
```

### Example 3: Single Table (Backward Compatibility)

```hocon
source {
  Paimon {
    warehouse = "file:///tmp/paimon"
    catalog_name = "paimon_catalog"
    catalog_type = "FILESYSTEM"
    database = "test_db"
    table = "users"
    query = "SELECT * FROM users WHERE id > 100"
    schema = {
      fields {
        id = BIGINT
        name = STRING
        email = STRING
      }
    }
  }
}
```

## Notes

1. **Exclusive Options**: You cannot use both `table` and `table_list` in the same configuration. Use `table_list` for multi-table reading and `table` for single table reading.

2. **Table Identification**: When using multi-table reading, each `SeaTunnelRow` will have its `tableId` set to the table path (e.g., "database.table"), allowing downstream processors to identify the source table.

3. **Schema Consistency**: While different tables can have different schemas, make sure your downstream transforms and sinks can handle the varying schemas appropriately.

4. **Performance**: Multi-table reading creates separate readers for each table, so consider the performance implications when reading from many tables simultaneously.

5. **Backward Compatibility**: Existing single-table configurations will continue to work without any changes.
