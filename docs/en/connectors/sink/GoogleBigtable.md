import ChangeLog from '../changelog/connector-google-bigtable.md';

# GoogleBigtable

> Google Bigtable sink connector

## Support Those Engines

> SeaTunnel Zeta<br/>

## Description

Writes data to Google Cloud Bigtable using the native Bigtable Data v2 Java client.

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Options

| name                | type    | required | default value |
|---------------------|---------|----------|---------------|
| project_id          | string  | yes      | -             |
| instance_id         | string  | yes      | -             |
| table               | string  | yes      | -             |
| rowkey_column       | list    | yes      | -             |
| column_family       | config  | yes      | -             |
| credentials_path    | string  | no       | -             |
| rowkey_delimiter    | string  | no       | ""            |
| version_column      | string  | no       | -             |
| null_mode           | string  | no       | skip          |
| batch_mutation_size | int     | no       | 100           |
| schema_save_mode    | enum    | no       | RECREATE_SCHEMA |
| data_save_mode      | enum    | no       | APPEND_DATA   |
| multi_table_sink_replica | int | no       | 1             |
| common-options      |         | no       | -             |

### project_id [string]

Google Cloud project ID. Example: `"my-gcp-project"`

### instance_id [string]

Bigtable instance ID. Example: `"my-bigtable-instance"`

### table [string]

The Bigtable table name to write to. Example: `"my-table"`. The connector does not create the Bigtable table; create it (with all required column families) before running the job.

### rowkey_column [list]

Column names used to compose the Bigtable row key. Example: `["id"]` or `["tenant", "id"]`.

When multiple columns are specified they are joined with `rowkey_delimiter`. With a single row-key column, a null or empty value fails the job with `WRITE_FAILED`. With multiple row-key columns, a null value in any non-last column silently becomes an empty segment in the composed row key (joined by `rowkey_delimiter`); only when the entire composed key is empty does the job fail.

### column_family [config]

Mapping from column name to column family name. Use `all_columns` as key to set a default family for all unmapped columns.

```hocon
column_family {
  name = "info"
  age  = "stats"
}
```

or to put everything in one family:

```hocon
column_family {
  all_columns = "cf"
}
```

Field names that do not appear in the map fall back to the `all_columns` family, or to the default family `cf` if `all_columns` is not configured.

### credentials_path [string]

Path to the Google Cloud service account JSON key file.

If not set, [Application Default Credentials (ADC)](https://cloud.google.com/docs/authentication/application-default-credentials) will be used — this works automatically on GCE/GKE or when `GOOGLE_APPLICATION_CREDENTIALS` is set in the environment.

### rowkey_delimiter [string]

Delimiter used to join multiple row-key column values. Default is `""` (empty string, no delimiter).

### version_column [string]

Column name whose `BIGINT` value is used as the Bigtable cell timestamp (microseconds since epoch). If not set, the current system time is used.

### null_mode [string]

How to handle `null` field values. Supported: `skip` (default), `empty`.

- `skip` — the cell is omitted from the mutation
- `empty` — an empty byte array is written to the cell

### batch_mutation_size [int]

Number of row mutations to accumulate before sending a BulkMutation to Bigtable. Default is `100`. Increase for higher throughput at the cost of higher per-task memory usage.

### schema_save_mode [enum]

Schema save mode. Only `RECREATE_SCHEMA` is supported now.

The connector does not create Bigtable tables or column families. Create the target table and all column families before the job starts.

### data_save_mode [enum]

Data save mode. Only `APPEND_DATA` is supported now.

`DROP_DATA` and `ERROR_WHEN_DATA_EXISTS` are not implemented for this connector. If you need a clean target, truncate or recreate the Bigtable table before running the job.

### multi_table_sink_replica [int]

The number of sink replicas used for multi-table writing. For details, see [Sink Common Options](../common-options/sink-common-options.md). `multi_table_sink_replica` increases the number of parallel writer replicas within a single sink instance; the target Bigtable table is fixed by the `table` option and is not derived per upstream table.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Data Type Mapping

All SeaTunnel types are supported:

| SeaTunnel type               | Storage format in Bigtable      |
|------------------------------|---------------------------------|
| TINYINT                      | 1-byte binary                   |
| SMALLINT                     | 2-byte big-endian binary        |
| INT                          | 4-byte big-endian binary        |
| BIGINT                       | 8-byte big-endian binary        |
| FLOAT                        | 4-byte IEEE 754 big-endian      |
| DOUBLE                       | 8-byte IEEE 754 big-endian      |
| BOOLEAN                      | 1-byte (1 = true, 0 = false)    |
| BYTES                        | Raw bytes                       |
| STRING                       | UTF-8 text                      |
| DECIMAL                      | UTF-8 plain string              |
| DATE                         | UTF-8 `yyyy-MM-dd`              |
| TIME                         | UTF-8 `HH:mm:ss`                |
| TIMESTAMP                    | UTF-8 `yyyy-MM-dd HH:mm:ss`     |

:::tip

Bigtable does not have relational columns. The sink writes every non-row-key field as a Bigtable cell. The target column family is selected by `column_family`; the Bigtable qualifier is the SeaTunnel field name. The sink treats every upstream row as an unconditional cell mutation, so `UPDATE` / `DELETE` row kinds are not interpreted as CDC operations and overwrite the previous cell under the same `(row key, column family, qualifier)` triple.

:::

## Task Example

### Basic — Application Default Credentials

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

sink {
  GoogleBigtable {
    project_id  = "my-gcp-project"
    instance_id = "my-bigtable-instance"
    table       = "events"
    rowkey_column = ["event_id"]
    column_family {
      all_columns = "cf"
    }
  }
}
```

### Service Account Key File

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

sink {
  GoogleBigtable {
    project_id       = "my-gcp-project"
    instance_id      = "my-bigtable-instance"
    table            = "events"
    credentials_path = "/secrets/sa-key.json"
    rowkey_column    = ["tenant_id", "event_id"]
    rowkey_delimiter = "#"
    column_family {
      all_columns = "data"
    }
    batch_mutation_size = 500
  }
}
```

### Multiple Column Families

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

sink {
  GoogleBigtable {
    project_id  = "my-gcp-project"
    instance_id = "my-bigtable-instance"
    table       = "user_profile"
    rowkey_column = ["user_id"]
    column_family {
      name        = "identity"
      email       = "identity"
      age         = "stats"
      last_login  = "stats"
    }
  }
}
```

### Use a version column and empty null values

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

sink {
  GoogleBigtable {
    project_id       = "my-gcp-project"
    instance_id      = "my-bigtable-instance"
    table            = "events"
    rowkey_column    = ["tenant_id", "event_id"]
    rowkey_delimiter = "#"
    version_column   = "event_ts"
    null_mode        = "empty"
    column_family {
      all_columns = "data"
      event_type  = "meta"
    }
  }
}
```

### Streaming write with checkpoint flush

In streaming mode, the writer flushes the in-memory mutation buffer at every checkpoint. The current `batch_mutation_size` still controls the in-task buffer; checkpoint frequency only affects how often already buffered mutations are sent to Bigtable.

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  FakeSource {
    row.num = 1000
    schema {
      fields {
        tenant_id  = string
        event_id   = string
        event_ts   = bigint
        event_type = string
        payload    = string
      }
    }
    plugin_output = "events_stream"
  }
}

sink {
  GoogleBigtable {
    plugin_input = "events_stream"
    project_id   = "my-gcp-project"
    instance_id  = "my-bigtable-instance"
    table        = "events"
    credentials_path = "/secrets/sa-key.json"
    rowkey_column    = ["tenant_id", "event_id"]
    rowkey_delimiter = "#"
    version_column   = "event_ts"
    column_family {
      all_columns = "data"
      event_type  = "meta"
    }
    batch_mutation_size = 200
  }
}
```

## Changelog

<ChangeLog />
