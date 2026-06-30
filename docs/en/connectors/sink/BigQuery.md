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

### Authentication Options

You must provide **one** of the following authentication methods:

1. **service_account_key_path**: Path to service account JSON file
2. **service_account_key_json**: Inline JSON key content
3. **Default credentials**: Uses application default credentials (ADC) if neither is specified

### Table Options

The target BigQuery table must already exist.
The connector reads the existing table schema during writer initialization and does not create the table automatically.

#### sequence_number_column

`sequence_number_column` is optional.

When `sequence_number_column` is configured, the value from that column is sent as `_CHANGE_SEQUENCE_NUMBER` to BigQuery, enabling BigQuery-side deduplication. On source retransmission, rows with the same primary key and sequence number can be deduplicated by BigQuery.
If `sequence_number_column` is not configured, `_CHANGE_SEQUENCE_NUMBER` is not sent and BigQuery will not perform sequence-number-based deduplication.

> **Note**
> - The `sequence_number_column` should reference a monotonically increasing column in your source table (e.g., `updated_at` as epoch millis, `version`, or `seq_id`). The column value must be of a type convertible to `long`.
> - To enable BigQuery-side deduplication in streaming mode, the target BigQuery table must have a Primary Key defined. Otherwise, BigQuery will treat every write as an append operation, regardless of the sequence number.

## Task Example

### Simple Example (Using Service Account File)

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 1000
    schema = {
      fields {
        user_id = "bigint"
        username = "string"
        email = "string"
        created_at = "timestamp"
      }
    }
  }
}

sink {
  BigQuery {
    project_id = "my-gcp-project"
    dataset_id = "analytics"
    table_id = "user_events"
    service_account_key_path = "/path/to/key.json"
    batch_size = 1000
  }
}
```

### CDC Streaming Mode (MySQL to BigQuery)

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

### Testing

This connector uses the BigQuery Storage Write API. The current local BigQuery emulator does not fully support the write path used by this connector.
For now, the connector should be tested against a real BigQuery environment.

## Changelog

<ChangeLog />