import ChangeLog from '../changelog/connector-google-bigtable.md';

# GoogleBigtable

> Google Bigtable source connector

## Support Those Engines

> SeaTunnel Zeta<br/>

## Description

Reads data from Google Cloud Bigtable using the native Bigtable Data v2 Java client.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)

:::tip

The source is bounded. The enumerator calls Bigtable `sampleRowKeys` to cut the table (or the configured `start_rowkey` / `end_rowkey` range) into tablet-sized splits, then assigns them by `hash(splitId) % parallelism`. Set `env.parallelism` (or the source parallelism) greater than 1 so multiple readers scan different key ranges. If sampling fails, returns no keys, or intersects to an empty range, the connector falls back to a single split so the job can still run. Each scan reads every requested cell for the configured row range and emits one SeaTunnel row per Bigtable row.

:::

## Options

| name             | type   | required | default value |
|------------------|--------|----------|---------------|
| project_id       | string | yes      | -             |
| instance_id      | string | yes      | -             |
| table            | string | yes      | -             |
| credentials_path | string | no       | -             |
| rowkey_column    | list   | no       | -             |
| start_rowkey     | string | no       | -             |
| end_rowkey       | string | no       | -             |
| start_timestamp  | long   | no       | -             |
| end_timestamp    | long   | no       | -             |
| max_versions     | int    | no       | 1             |
| scan_row_limit   | int    | no       | -1            |
| common-options   |        | no       | -             |

### project_id [string]

Google Cloud project ID.

### instance_id [string]

Bigtable instance ID.

### table [string]

Bigtable table name to read from.

### credentials_path [string]

Path to the Google Cloud service account JSON key file. If omitted, Application Default Credentials (ADC) are used. ADC works automatically on GCE/GKE nodes, in `gcloud` shell sessions, or when the `GOOGLE_APPLICATION_CREDENTIALS` environment variable points to a service account JSON file.

### rowkey_column [list]

Optional list of field names that should receive the row key value. If this option is not set, the connector uses a schema field named `rowkey` as the row-key field.

Each listed field is decoded independently according to its own declared type in `schema.fields`: `BYTES` receives the raw row-key bytes; `STRING` receives a UTF-8 decoded view. Different row-key fields can therefore use different types in the same scan (for example one field exposing the raw key bytes for downstream binary processing, another exposing a UTF-8 view).

### start_rowkey [string]

Inclusive start row key for the scan. If not set, the scan starts from the beginning of the table.

The connector passes the value to the Bigtable client as a UTF-8 string; only lexicographic comparison is supported. Use `BYTES` for binary row keys that do not encode as UTF-8.

### end_rowkey [string]

Exclusive end row key for the scan. If not set, the scan reads to the end of the table.

### start_timestamp [long]

Inclusive start timestamp filter (microseconds since epoch). Combined with `end_timestamp` and `max_versions`, this controls which cell versions Bigtable returns for each column qualifier.

### end_timestamp [long]

Exclusive end timestamp filter (microseconds since epoch).

### max_versions [int]

Maximum number of cell versions to return per column qualifier. Default `1` returns only the latest version. Larger values expose historical cell versions; the source still emits one row per Bigtable row, so older versions of the same cell are flattened into the latest returned cell.

### scan_row_limit [int]

Maximum number of rows to return **per split**. `-1` (default) means no limit. When the enumerator produces multiple splits, the job-level upper bound is about `scan_row_limit × split count`, not a single table-wide cap. Use this option together with `start_rowkey` / `end_rowkey` to do paginated full-table scans across multiple jobs.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Schema Mapping

Field names in the SeaTunnel schema must follow the pattern `familyName:qualifier`, for example `cf:name` or `stats:age`. The row-key field is controlled by `rowkey_column`; if it is not configured, the special field name `rowkey` maps to the Bigtable row key.

| Schema field name | Mapped Bigtable cell        |
|-------------------|-----------------------------|
| `rowkey`          | Row key                     |
| `cf:name`         | Column family `cf`, qualifier `name` |
| `stats:age`       | Column family `stats`, qualifier `age` |

:::tip

The source reads the latest returned cell for each `family:qualifier` field. Use `start_timestamp`, `end_timestamp`, and `max_versions` to control the Bigtable scan filter. SeaTunnel field types must match the bytes stored in Bigtable. For example, numeric values written by this connector are binary big-endian values, while `STRING`, `DATE`, `TIME`, `TIMESTAMP`, and `DECIMAL` are UTF-8 text.

:::

## Task Example

### Read all rows with Application Default Credentials

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  GoogleBigtable {
    project_id  = "my-gcp-project"
    instance_id = "my-bigtable-instance"
    table       = "events"
    schema {
      fields {
        rowkey    = BYTES
        "cf:type" = STRING
        "cf:ts"   = BIGINT
      }
    }
  }
}
```

### Scan a row-key range with a service account

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  GoogleBigtable {
    project_id       = "my-gcp-project"
    instance_id      = "my-bigtable-instance"
    table            = "events"
    credentials_path = "/secrets/sa-key.json"
    start_rowkey     = "2024-01-01#"
    end_rowkey       = "2024-02-01#"
    max_versions     = 1
    schema {
      fields {
        rowkey    = STRING
        "cf:type" = STRING
        "cf:data" = STRING
      }
    }
  }
}
```

### Use a custom row-key field name

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  GoogleBigtable {
    project_id    = "my-gcp-project"
    instance_id   = "my-bigtable-instance"
    table         = "events"
    rowkey_column = ["event_id"]
    schema {
      fields {
        event_id  = STRING
        "cf:type" = STRING
        "cf:data" = STRING
      }
    }
  }
}
```

### Bounded streaming scan with cell-version filtering

Use `STREAMING` job mode when you want the scan to run with checkpointing while still being a single bounded read. Combine `start_timestamp`, `end_timestamp`, and `max_versions` to restrict which cell versions Bigtable returns.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  GoogleBigtable {
    project_id      = "my-gcp-project"
    instance_id     = "my-bigtable-instance"
    table           = "events"
    start_timestamp = 1704067200000000
    end_timestamp   = 1735689600000000
    max_versions    = 3
    scan_row_limit  = 500000
    schema {
      fields {
        rowkey    = STRING
        "cf:type" = STRING
        "cf:data" = STRING
        "cf:ts"   = BIGINT
      }
    }
  }
}
```

## Changelog

<ChangeLog />
