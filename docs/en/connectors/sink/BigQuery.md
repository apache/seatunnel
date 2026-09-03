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
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
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
| batch_size                  | int     | No       | 1000    | Number of rows to batch before sending to BigQuery                                                          |
| emulator_host               | string  | No       | -       | BigQuery emulator host, such as `localhost:9050`. This option is intended for tests only.                    |
| universe_domain             | string  | No       | -       | The Google Cloud Universe Domain, such as `s3nsapis.fr` for S3NS sovereign cloud.                           |
| schema_save_mode            | enum    | No       | CREATE_SCHEMA_WHEN_NOT_EXIST | Schema save mode. See below.                                                                           |
| data_save_mode              | enum    | No       | APPEND_DATA | Data save mode. See below.                                                                                 |
| custom_sql                  | string  | No       | -       | Custom SQL to execute when `data_save_mode` is `CUSTOM_PROCESSING`.                                         |
| multi_table_sink_replica    | int     | No       | -       | Sink common option. It controls sink replica count in multi-table runtime.                                  |
| common-options              |         | No       | -       | Sink common options. See [Sink Common Options](../common-options/sink-common-options.md).                    |

### Authentication Options

For production BigQuery jobs, provide **one** of the following authentication methods. Authentication is skipped only when `emulator_host` is configured for tests.

1. **service_account_key_path**: Path to service account JSON file
2. **service_account_key_json**: Inline JSON key content
3. **Default credentials**: Uses application default credentials (ADC) if neither is specified

### Table Options

The target BigQuery table can be created automatically using SeaTunnel's SaveMode.
By configuring `schema_save_mode` to `CREATE_SCHEMA_WHEN_NOT_EXIST` (default) or `RECREATE_SCHEMA`, the connector can automatically create the BigQuery dataset and table based on the upstream schema information.

The connector writes to target tables determined by `project_id.dataset_id.table_id`.
In multi-table pipelines, you can configure `table_id` to include `${table_name}` (e.g., `table_id = "${table_name}"` or `table_id = "prefix_${table_name}"`) to dynamically route data to different BigQuery tables. Under this multi-table setting, the connector automatically creates separate target tables as needed.

### schema_save_mode [Enum]

Before the synchronization task starts, controls how the target table schema is handled.
- `RECREATE_SCHEMA` : Drop the target table if it exists, and then recreate it.
- `CREATE_SCHEMA_WHEN_NOT_EXIST` : Create the target table if it does not exist, or skip creation if it exists.
- `ERROR_WHEN_SCHEMA_NOT_EXIST` : Throw an error if the target table does not exist.
- `IGNORE` : Ignore schema handling and do not perform any schema-related checks or DDL actions.

### data_save_mode [Enum]

Before the synchronization task starts, controls how existing data on the target side is handled.
- `DROP_DATA` : Delete existing data in the target table.
- `APPEND_DATA` : Keep the target table's existing structure and append new data.
- `CUSTOM_PROCESSING` : Perform user-defined processing. This requires configuring `custom_sql`.
- `ERROR_WHEN_DATA_EXISTS` : Throw an error if the target table already contains data.

### custom_sql [String]

When `data_save_mode` is set to `CUSTOM_PROCESSING`, the SQL statement specified here will be executed before row writing begins.

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

`emulator_host` is only for local or CI tests. When it is configured, SeaTunnel connects to the emulator without Google credentials. Do not use this option for production BigQuery jobs.

## Task Example

### Simple Batch Example

This example shows a local end-to-end batch test against the BigQuery emulator.
Production jobs should provide a real service-account key (or rely on default
application credentials) and target a real GCP project.

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
  }
}

sink {
  BigQuery {
    project_id = "my-gcp-project"
    dataset_id = "cdc_dataset"
    table_id = "orders"
    service_account_key_path = "/path/to/key.json"
    write_mode = "streaming"
    batch_size = 500
  }
}
```

When the upstream CDC source already produces a monotonically increasing column
(such as an `updated_at` epoch millis or a row version), wire it to
`sequence_number_column` so BigQuery can dedup retried batches. The target
table must define a primary key (the example above uses `PRIMARY KEY (uuid)
NOT ENFORCED`), otherwise BigQuery treats every write as an append and skips
deduplication.

```hocon
sink {
  BigQuery {
    project_id = "my-gcp-project"
    dataset_id = "cdc_dataset"
    table_id = "orders"
    service_account_key_path = "/path/to/key.json"
    write_mode = "streaming"
    sequence_number_column = "updated_at"
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

### Inline Service Account Key

For environments where a key file is inconvenient (CI runners, Kubernetes
secrets mounted as env vars), inline the JSON content with
`service_account_key_json`.

```hocon
sink {
  BigQuery {
    project_id = "my-gcp-project"
    dataset_id = "orders"
    table_id = "customer_orders"
    service_account_key_json = "${GCP_SA_KEY_JSON}"
    batch_size = 500
  }
}
```

### Testing

This connector uses the BigQuery Storage Write API. The current local BigQuery emulator does not fully support the write path used by this connector.
Use `emulator_host` only for local or CI checks that are compatible with the emulator. Production validation should be done against a real BigQuery environment.

## Changelog

<ChangeLog />
