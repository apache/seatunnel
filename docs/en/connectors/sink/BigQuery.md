import ChangeLog from '../changelog/connector-bigquery.md';

# BigQuery

> BigQuery sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Key Features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md) for batch mode only
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] schema evolution (`ADD COLUMN` only)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Description

Sink connector for Google Cloud BigQuery using the Storage Write API for high-performance data ingestion.

## Supported DataSource Info

| Datasource | Supported Versions | Maven                                                                                  |
|------------|--------------------|----------------------------------------------------------------------------------------|
| BigQuery   | BOM 26.72.0        | [Download](https://mvnrepository.com/artifact/com.google.cloud/google-cloud-bigquery) |


## Options

| Name                        | Type    | Required | Default | Description                                                                                                 |
|-----------------------------|---------|----------|---------|-------------------------------------------------------------------------------------------------------------|
| project_id                  | string  | Yes      | -       | GCP project ID                                                                                              |
| dataset_id                  | string  | Yes      | -       | BigQuery dataset ID                                                                                         |
| table_id                    | string  | Yes      | -       | BigQuery table ID                                                                                           |
| service_account_key_path    | string  | No       | -       | Path to GCP service account JSON key file                                                                   |
| service_account_key_json    | string  | No       | -       | Inline GCP service account JSON key content                                                                 |
| write_mode                  | string  | No       | batch   | Write mode. Supported values: `batch` and `streaming`                                                       |
| sequence_number_column      | string  | No       | -       | Column name used as sequence number for CDC deduplication. Only applicable when `write_mode` is `streaming` |
| schema_evolution_enabled    | boolean | No       | false   | Whether to apply `ADD COLUMN` schema change events to the target BigQuery table                             |
| schema_evolution_relax_not_null | boolean | No    | false   | Whether to add non-null source columns as `NULLABLE` BigQuery fields during schema evolution                |
| batch_size                  | int     | No       | 1000    | Number of rows to batch before sending to BigQuery                                                          |
| emulator_host               | string  | No       | -       | BigQuery emulator REST host, such as `localhost:9050`. This option is intended for tests only.               |
| emulator_grpc_host          | string  | No       | -       | BigQuery emulator Storage Write API host, such as `localhost:9060`. Falls back to `emulator_host`. Tests only. |
| multi_table_sink_replica    | int     | No       | -       | Sink common option. It controls sink replica count in multi-table runtime, but this connector still writes to the single configured BigQuery table. |
| common-options              |         | No       | -       | Sink common options. See [Sink Common Options](../common-options/sink-common-options.md).                    |

### Authentication Options

For production BigQuery jobs, provide **one** of the following authentication methods. Authentication is skipped only when `emulator_host` is configured for tests.

1. **service_account_key_path**: Path to service account JSON file
2. **service_account_key_json**: Inline JSON key content
3. **Default credentials**: Uses application default credentials (ADC) if neither is specified

### Table Options

The target BigQuery table must already exist.
The connector reads the existing table schema during writer initialization and does not create the table automatically.

The connector writes to one configured table: `project_id.dataset_id.table_id`. It does not create a different BigQuery table for each upstream table. For multi-table pipelines, configure separate sink entries or route data before the BigQuery sink.

### Schema Evolution

Schema evolution is disabled by default. Set `schema_evolution_enabled = true` on the BigQuery sink and `schema-changes.enabled = true` on a supported CDC source to propagate source `ADD COLUMN` events to the configured target table.

Only physical `ADD COLUMN` events are supported. By default, added scalar or struct columns must be nullable. Set `schema_evolution_relax_not_null = true` to add a non-null source scalar or struct column as a `NULLABLE` BigQuery field. This relaxation is useful because historical target rows have no value for a newly added source column.

Source array columns must be non-null and are created as BigQuery `REPEATED` fields. Nullable source arrays are rejected because BigQuery arrays cannot be `NULL`; silently mapping them would lose the distinction between `NULL` and an empty array. `DROP COLUMN`, `RENAME COLUMN`, and `MODIFY COLUMN` are not supported. BigQuery appends new fields to the target schema, so source `FIRST` and `AFTER` position hints do not change the physical BigQuery field order. Rows are encoded by field name, and the sink refreshes its writer schema before accepting rows that use the new column.

An unsupported schema change fails the job instead of being silently skipped, because continuing with different source and target schemas can misroute or corrupt subsequent rows. Restoring from the same checkpoint can replay the unsupported event and fail again. Before restarting, reconcile the source and BigQuery schemas, then restart from a source position that does not replay the unsupported event. If the pipeline can produce unsupported DDL, disable `schema-changes.enabled` and manage those schema changes outside SeaTunnel.

Schema updates use `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`. If the target already contains a field with the same name, its type and mode must be compatible or the job fails. In addition to the permissions required for Storage Write API ingestion, the credentials must be able to run the DDL job and read the resulting table metadata.

### Write Modes

- `batch`: uses BigQuery buffered write streams and commits data during SeaTunnel checkpoint/commit. This is the mode covered by the exactly-once feature mark.
- `streaming`: uses the default stream and writes CDC records with BigQuery change fields. This mode is suitable for CDC upsert/delete records, but it is not marked as exactly-once by this connector.

For CDC writes in `streaming` mode, prepare the target BigQuery table with a primary key before starting the SeaTunnel job. The connector maps SeaTunnel row kinds to BigQuery change records: `INSERT` and `UPDATE_AFTER` are written as `UPSERT`, while `DELETE` and `UPDATE_BEFORE` are written as `DELETE`.

#### sequence_number_column

`sequence_number_column` is optional.

When `sequence_number_column` is configured, the value from that column is sent as `_CHANGE_SEQUENCE_NUMBER` to BigQuery, enabling BigQuery-side deduplication. On source retransmission, rows with the same primary key and sequence number can be deduplicated by BigQuery.
If `sequence_number_column` is not configured, `_CHANGE_SEQUENCE_NUMBER` is not sent and BigQuery will not perform sequence-number-based deduplication.

> **Note**
> - BigQuery requires `_CHANGE_SEQUENCE_NUMBER` to be a hexadecimal `STRING`. For integer columns and exact integral decimal values, such as MySQL `BIGINT UNSIGNED` mapped to `DECIMAL(20, 0)`, the connector converts non-negative values in the unsigned 64-bit range to hexadecimal strings. For string columns, values are treated as already-encoded hexadecimal sequence numbers and are validated without conversion.
> - A sequence number may contain up to four sections separated by `/`, and each section may contain up to 16 hexadecimal characters. Null, negative, empty, or malformed values are rejected.
> - The `sequence_number_column` should reference a monotonically increasing column in your source table (e.g., `updated_at` as epoch millis, `version`, or `seq_id`).
> - To enable BigQuery-side deduplication in streaming mode, the target BigQuery table must have a Primary Key defined. Otherwise, BigQuery will treat every write as an append operation, regardless of the sequence number.

### emulator_host

`emulator_host` is only for local or CI tests and configures the emulator REST endpoint. When it is configured, SeaTunnel connects to the emulator without Google credentials. Set `emulator_grpc_host` when the emulator exposes its Storage Write API on a different endpoint, as goccy BigQuery emulator does by default on port `9060`. If omitted, the gRPC endpoint falls back to `emulator_host`. Do not use these options for production BigQuery jobs.

## Task Example

### Simple Batch Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 10
    string.fake.mode = "template"
    string.template = ["key", "value"]
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
        c_time = time
      }
    }
  }
}

sink {
  BigQuery {
    project_id = "test-project"
    dataset_id = "test_dataset"
    table_id = "test_table"
    batch_size = 2
    emulator_host = "localhost:9050"
    emulator_grpc_host = "localhost:9060"
  }
}
```

### CDC Streaming Mode (MySQL to BigQuery)

The target BigQuery table should already exist and should define the primary key used by the CDC source. For example:

```sql
CREATE TABLE `my-gcp-project.cdc_dataset.orders` (
  uuid INT64 NOT NULL,
  name STRING,
  score INT64,
  PRIMARY KEY (uuid) NOT ENFORCED
)
OPTIONS (max_staleness = INTERVAL 0 MINUTE);
```

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  MySQL-CDC {
      parallelism = 1
      server-id = 5652
      username = "st_user_source"
      password = "mysqlpw"
      table-names = ["mysql_cdc.mysql_cdc_e2e_source_table"]
      url = "jdbc:mysql://mysql_cdc_e2e:3306/mysql_cdc"
      schema-changes.enabled = true
  }
}

sink {
  BigQuery {
    project_id = "my-gcp-project"
    dataset_id = "cdc_dataset"
    table_id = "orders"
    service_account_key_path = "/path/to/key.json"
    write_mode = "streaming"
    schema_evolution_enabled = true
    batch_size = 500
  }
}
```

### Complex Data Types Example

```hocon
source {
  FakeSource {
    row.num = 100
    schema = {
      fields {
        order_id = "bigint"
        customer = {
          name = "string"
          email = "string"
        }
        items = "array<string>"
        metadata = "map<string, string>"
        order_date = "date"
      }
    }
  }
}

sink {
  BigQuery {
    project_id = "my-gcp-project"
    dataset_id = "orders"
    table_id = "customer_orders"
    service_account_key_path = "/path/to/key.json"
    batch_size = 500
  }
}
```

### Testing

This connector uses both the BigQuery REST API and Storage Write API. For goccy BigQuery emulator, configure `emulator_host` with its REST port (`9050` by default) and `emulator_grpc_host` with its gRPC port (`9060` by default).
The emulator is suitable for local and CI coverage, but production validation should still be done against real BigQuery.

## Changelog

<ChangeLog />
