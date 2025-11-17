# Metadata

> Metadata transform plugin

## Description

The Metadata transform plugin is used to extract metadata information from data rows and convert it into regular fields for subsequent processing and analysis.

**Core Features:**
- Extracts metadata (such as database name, table name, row type, etc.) as visible fields
- Supports custom output field names
- Does not modify original data fields, only adds metadata fields

**Typical Use Cases:**
- Recording data source (database name, table name) during CDC data synchronization
- Tracking data change types (INSERT, UPDATE, DELETE)
- Recording event time and delay information of data
- Identifying data sources when merging multiple tables

## Supported Metadata Fields

|    Metadata Key    | Output Type |          Description          | Data Source |
|:---------:|:--------:|:-----------------------------:|:----:|
| Database  |  string  |  Name of the database containing the data  | All connectors |
|   Table   |  string  |  Name of the table containing the data  | All connectors |
|  RowKind  |  string  |  Row change type, values: +I (insert), -U (update before), +U (update after), -D (delete)  | All connectors |
| EventTime |   long   |  Event timestamp of data change (milliseconds)  | CDC connectors |
|   Delay   |   long   |  Data collection delay time (milliseconds), i.e., the difference between data extraction time and database change time  | CDC connectors |
| Partition |  string  |  Partition information of the data, multiple partition fields separated by commas  | Connectors supporting partitions |

### Important Notes

1. **Metadata field names are case-sensitive**: Configuration must strictly follow the Key names in the table above (e.g., `Database`, `Table`, `RowKind`, etc.)
2. **CDC-specific fields**: `EventTime` and `Delay` are only valid when using CDC connectors (except TiDB-CDC)

## Options

|      name       | type | required | default value | description       |
|:---------------:|------|:--------:|:-------------:|-------------------|
| metadata_fields | map  |    no     |   empty map   | Mapping relationship between metadata fields and output fields, format: `Metadata Key = output field name` |

### metadata_fields [map]

Defines the mapping relationship between metadata fields and output fields.

**Configuration Format:**
```hocon
metadata_fields {
  <Metadata Key> = <output field name>
  <Metadata Key> = <output field name>
  ...
}
```

**Configuration Example:**
```hocon
metadata_fields {
  Database = source_db      # Map database name to source_db field
  Table = source_table      # Map table name to source_table field
  RowKind = op_type         # Map row type to op_type field
  EventTime = event_ts      # Map event time to event_ts field
  Delay = sync_delay        # Map delay time to sync_delay field
  Partition = partition_info # Map partition info to partition_info field
}
```

**Notes:**
- The left side must be a supported metadata Key (see table above), and is strictly case-sensitive
- The right side is a custom output field name, which cannot duplicate existing field names
- You can select only the metadata fields you need, not all of them must be configured

## Complete Examples

### Example 1: MySQL CDC Data Synchronization, Extracting All Metadata

Synchronizing data from MySQL database and extracting all available metadata information.

```yaml
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    plugin_output = "mysql_cdc_source"
    server-id = 5652
    username = "root"
    password = "your_password"
    table-names = ["mydb.users"]
    url = "jdbc:mysql://localhost:3306/mydb"
  }
}

transform {
  Metadata {
    plugin_input = "mysql_cdc_source"
    plugin_output = "metadata_added"
    metadata_fields {
      Database = source_database    # Extract database name
      Table = source_table          # Extract table name
      RowKind = change_type         # Extract change type
      EventTime = event_timestamp   # Extract event time
      Delay = sync_delay_ms         # Extract sync delay
    }
  }
}

sink {
  Console {
    plugin_input = "metadata_added"
  }
}
```

**Input Data Example:**
```
Original data row (from mydb.users table):
id=1, name="John", age=25
RowKind: +I (INSERT)
```

**Output Data Example:**
```
Transformed data row:
id=1, name="John", age=25, source_database="mydb", source_table="users",
change_type="+I", event_timestamp=1699000000000, sync_delay_ms=100
```

---

### Example 2: Extracting Only Partial Metadata

Extracting only data source information (database name and table name) for multi-table merge scenarios.

```yaml
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  MySQL-CDC {
    plugin_output = "multi_table_source"
    server-id = 5652
    username = "root"
    password = "your_password"
    table-names = ["db1.orders", "db2.orders"]
    url = "jdbc:mysql://localhost:3306"
  }
}

transform {
  Metadata {
    plugin_input = "multi_table_source"
    plugin_output = "with_source_info"
    metadata_fields {
      Database = db_name
      Table = table_name
    }
  }
}

sink {
  Jdbc {
    plugin_input = "with_source_info"
    url = "jdbc:mysql://localhost:3306/target_db"
    table = "merged_orders"
    # Target table will contain db_name and table_name fields to identify data source
  }
}
```
