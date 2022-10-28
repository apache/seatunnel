# Apache Iceberg

> Apache Iceberg source connector

## Description

Source connector for Apache Iceberg. It can support batch and stream mode.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

- [x] data format
    - [x] parquet
    - [x] orc
    - [x] avro
- [x] iceberg catalog
    - [x] hadoop(2.7.5)
    - [x] hive(2.3.9)

##  Options

| name                     | type    | required | default value        |
| ------------------------ | ------- | -------- | -------------------- |
| catalog_name             | string  | yes      | -                    |
| catalog_type             | string  | yes      | -                    |
| uri                      | string  | no       | -                    |
| warehouse                | string  | yes      | -                    |
| namespace                | string  | yes      | -                    |
| table                    | string  | yes      | -                    |
| case_sensitive           | boolean | no       | false                |
| start_snapshot_timestamp | long    | no       | -                    |
| start_snapshot_id        | long    | no       | -                    |
| end_snapshot_id          | long    | no       | -                    |
| use_snapshot_id          | long    | no       | -                    |
| use_snapshot_timestamp   | long    | no       | -                    |
| stream_scan_strategy     | enum    | no       | FROM_LATEST_SNAPSHOT |
| common-options           |         | no       | -                    |

### catalog_name [string]

User-specified catalog name.

### catalog_type [string]

The optional values are:
- hive: The hive metastore catalog.
- hadoop: The hadoop catalog.

### uri [string]

The Hive metastore’s thrift URI.

### warehouse [string]

The location to store metadata files and data files.

### namespace [string]

The iceberg database name in the backend catalog.

### table [string]

The iceberg table name in the backend catalog.

### case_sensitive [boolean]

If data columns where selected via fields(Collection), controls whether the match to the schema will be done with case sensitivity.

### fields [array]

Use projection to select data columns and columns order.

### start_snapshot_id [long]

Instructs this scan to look for changes starting from a particular snapshot (exclusive).

### start_snapshot_timestamp [long]

Instructs this scan to look for changes starting from  the most recent snapshot for the table as of the timestamp. timestamp – the timestamp in millis since the Unix epoch

### end_snapshot_id [long]

Instructs this scan to look for changes up to a particular snapshot (inclusive).

### use_snapshot_id [long]

Instructs this scan to look for use the given snapshot ID.

### use_snapshot_timestamp [long]

Instructs this scan to look for use the most recent snapshot as of the given time in milliseconds. timestamp – the timestamp in millis since the Unix epoch

### stream_scan_strategy [enum]

Starting strategy for stream mode execution, Default to use `FROM_LATEST_SNAPSHOT` if don’t specify any value.
The optional values are:
- TABLE_SCAN_THEN_INCREMENTAL: Do a regular table scan then switch to the incremental mode.
- FROM_LATEST_SNAPSHOT: Start incremental mode from the latest snapshot inclusive.
- FROM_EARLIEST_SNAPSHOT: Start incremental mode from the earliest snapshot inclusive.
- FROM_SNAPSHOT_ID: Start incremental mode from a snapshot with a specific id inclusive.
- FROM_SNAPSHOT_TIMESTAMP: Start incremental mode from a snapshot with a specific timestamp inclusive.

### common options 

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

## Example

simple

```hocon
source {
  Iceberg {
    catalog_name = "seatunnel"
    catalog_type = "hadoop"
    warehouse = "hdfs://your_cluster//tmp/seatunnel/iceberg/"
    namespace = "your_iceberg_database"
    table = "your_iceberg_table"
  }
}
```
Or

```hocon
source {
  Iceberg {
    catalog_name = "seatunnel"
    catalog_type = "hive"
    uri = "thrift://localhost:9083"
    warehouse = "hdfs://your_cluster//tmp/seatunnel/iceberg/"
    namespace = "your_iceberg_database"
    table = "your_iceberg_table"
  }
}
```

schema projection

```hocon
source {
  Iceberg {
    catalog_name = "seatunnel"
    catalog_type = "hadoop"
    warehouse = "hdfs://your_cluster/tmp/seatunnel/iceberg/"
    namespace = "your_iceberg_database"
    table = "your_iceberg_table"

    fields {
      f2 = "boolean"
      f1 = "bigint"
      f3 = "int"
      f4 = "bigint"
    }
  }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Iceberg Source Connector
