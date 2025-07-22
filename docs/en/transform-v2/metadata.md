# Metadata

> Metadata transform plugin

## Description
Metadata transform plugin for adding metadata fields to data

## Available Metadata

|    Key    | DataType | Description                                                                                        |
|:---------:|:--------:|:---------------------------------------------------------------------------------------------------|
| Database  |  string  | Name of the table that contain the row.                                                            |
|   Table   |  string  | Name of the table that contain the row.                                                            |
|  RowKind  |  string  | The type of operation                                                                              |
| EventTime |   Long   | The time at which the connector processed the event.                                               |
|   Delay   |   Long   | The difference between data extraction time and database change time                               |
| Partition |  string  | Contains the partition field of the corresponding number table of the row, multiple using `,` join |

### note
    `Delay` `Partition` only worked on cdc series connectors for now , except TiDB-CDC

## Options

|      name       | type | required | default value | Description                                                               |
|:---------------:|------|----------|---------------|---------------------------------------------------------------------------|
| metadata_fields | map  | yes      |               | A mapping metadata input fields and their corresponding output fields.    |

### metadata_fields [map]

A mapping between metadata fields and their respective output fields. 

```hocon
metadata_fields {
  Database = c_database
  Table = c_table
  RowKind = c_rowKind
  EventTime = c_ts_ms
  Delay = c_delay
}
```

## Examples

```yaml

env {
    parallelism = 1
    job.mode = "STREAMING"
    checkpoint.interval = 5000
    read_limit.bytes_per_second = 7000000
    read_limit.rows_per_second = 400
}

source {
    MySQL-CDC {
        plugin_output = "customers_mysql_cdc"
        server-id = 5652
        username = "root"
        password = "zdyk_Dev@2024"
        table-names = ["source.user"]
        url = "jdbc:mysql://172.16.17.123:3306/source"
    }
}

transform {
  Metadata {
    metadata_fields {
      Database = database
      Table = table
      RowKind = rowKind
      EventTime = ts_ms
      Delay = delay
    }
    plugin_output = "trans_result"
  }
}

sink {
  Console {
    plugin_input = "custom_name"
  }
}

```

