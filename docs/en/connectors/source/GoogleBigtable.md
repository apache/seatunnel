import ChangeLog from '../changelog/connector-google-bigtable.md';

# GoogleBigtable

> Google Bigtable source connector

## Description

Reads data from Google Cloud Bigtable using the native Bigtable Data v2 Java client.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)

:::tip

The source is bounded. It creates one split for the configured table or row-key range, so increasing job parallelism does not split one Bigtable scan into multiple tablet-range reads.

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

Path to the Google Cloud service account JSON key file. If omitted, Application Default Credentials (ADC) are used.

### rowkey_column [list]

Optional list of field names that should receive the row key value. If this option is not set, the connector uses a schema field named `rowkey` as the row-key field. The row-key field can be declared as `BYTES` or `STRING`.

### start_rowkey [string]

Inclusive start row key for the scan. If not set, the scan starts from the beginning of the table.

### end_rowkey [string]

Exclusive end row key for the scan. If not set, the scan reads to the end of the table.

### start_timestamp [long]

Inclusive start timestamp filter (microseconds since epoch).

### end_timestamp [long]

Exclusive end timestamp filter (microseconds since epoch).

### max_versions [int]

Maximum number of cell versions to return per column qualifier. Default `1` returns only the latest version.

### scan_row_limit [int]

Maximum number of rows to return. `-1` (default) means no limit.

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

## Example

### Read all rows — Application Default Credentials

```hocon
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

## Changelog

<ChangeLog />
