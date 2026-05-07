import ChangeLog from '../changelog/connector-google-bigtable.md';

# GoogleBigtable

> Google Bigtable sink connector

## Description

Writes data to Google Cloud Bigtable using the native Bigtable Data v2 Java client.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [batch](../../introduction/concepts/connector-v2-features.md)

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
| common-options      |         | no       | -             |

### project_id [string]

Google Cloud project ID. Example: `"my-gcp-project"`

### instance_id [string]

Bigtable instance ID. Example: `"my-bigtable-instance"`

### table [string]

The Bigtable table name to write to. Example: `"my-table"`

### rowkey_column [list]

Column names used to compose the Bigtable row key. Example: `["id"]` or `["tenant", "id"]`.

When multiple columns are specified they are joined with `rowkey_delimiter`.

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

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Data Types

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

## Example

### Basic — Application Default Credentials

```hocon
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

## Changelog

<ChangeLog />
