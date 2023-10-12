# Apache Iceberg

> Apache Iceberg source connector

## Support Iceberg Version

- 0.14.0

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)
- [x] data format
  - [x] parquet
  - [x] orc
  - [x] avro
- [x] iceberg catalog
  - [x] hadoop(2.7.1 , 2.7.5 , 3.1.3)
  - [x] hive(2.3.9 , 3.1.2)

## Description

Source connector for Apache Iceberg. It can support batch and stream mode.

## Supported DataSource Info

| Datasource |      Dependent      |                                   Maven                                   |
|------------|---------------------|---------------------------------------------------------------------------|
| Iceberg    | flink-shaded-hadoop | [Download](https://mvnrepository.com/search?q=flink-shaded-hadoop-)       |
| Iceberg    | hive-exec           | [Download](https://mvnrepository.com/artifact/org.apache.hive/hive-exec)  |
| Iceberg    | libfb303            | [Download](https://mvnrepository.com/artifact/org.apache.thrift/libfb303) |

## Database Dependency

> In order to be compatible with different versions of Hadoop and Hive, the scope of hive-exec and flink-shaded-hadoop-2 in the project pom file are provided, so if you use the Flink engine, first you may need to add the following Jar packages to <FLINK_HOME>/lib directory, if you are using the Spark engine and integrated with Hadoop, then you do not need to add the following Jar packages.

```
flink-shaded-hadoop-x-xxx.jar
hive-exec-xxx.jar
libfb303-xxx.jar
```

> Some versions of the hive-exec package do not have libfb303-xxx.jar, so you also need to manually import the Jar package.

## Data Type Mapping

| Iceberg Data type | SeaTunnel Data type |
|-------------------|---------------------|
| BOOLEAN           | BOOLEAN             |
| INTEGER           | INT                 |
| LONG              | BIGINT              |
| FLOAT             | FLOAT               |
| DOUBLE            | DOUBLE              |
| DATE              | DATE                |
| TIME              | TIME                |
| TIMESTAMP         | TIMESTAMP           |
| STRING            | STRING              |
| FIXED<br/>BINARY  | BYTES               |
| DECIMAL           | DECIMAL             |
| STRUCT            | ROW                 |
| LIST              | ARRAY               |
| MAP               | MAP                 |

## Source Options

|           Name           |  Type   | Required |       Default        |                                                                                                                                                                                                                                                                                                      Description                                                                                                                                                                                                                                                                                                       |
|--------------------------|---------|----------|----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| catalog_name             | string  | yes      | -                    | User-specified catalog name.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| catalog_type             | string  | yes      | -                    | The optional values are: hive(The hive metastore catalog),hadoop(The hadoop catalog)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| uri                      | string  | no       | -                    | The Hive metastore’s thrift URI.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| warehouse                | string  | yes      | -                    | The location to store metadata files and data files.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| namespace                | string  | yes      | -                    | The iceberg database name in the backend catalog.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| table                    | string  | yes      | -                    | The iceberg table name in the backend catalog.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| schema                   | config  | no       | -                    | Use projection to select data columns and columns order.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| case_sensitive           | boolean | no       | false                | If data columns where selected via schema [config], controls whether the match to the schema will be done with case sensitivity.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| start_snapshot_timestamp | long    | no       | -                    | Instructs this scan to look for changes starting from  the most recent snapshot for the table as of the timestamp. <br/>timestamp – the timestamp in millis since the Unix epoch                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| start_snapshot_id        | long    | no       | -                    | Instructs this scan to look for changes starting from a particular snapshot (exclusive).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| end_snapshot_id          | long    | no       | -                    | Instructs this scan to look for changes up to a particular snapshot (inclusive).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| use_snapshot_id          | long    | no       | -                    | Instructs this scan to look for use the given snapshot ID.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| use_snapshot_timestamp   | long    | no       | -                    | Instructs this scan to look for use the most recent snapshot as of the given time in milliseconds. timestamp – the timestamp in millis since the Unix epoch                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| stream_scan_strategy     | enum    | no       | FROM_LATEST_SNAPSHOT | Starting strategy for stream mode execution, Default to use `FROM_LATEST_SNAPSHOT` if don’t specify any value,The optional values are:<br/>TABLE_SCAN_THEN_INCREMENTAL: Do a regular table scan then switch to the incremental mode.<br/>FROM_LATEST_SNAPSHOT: Start incremental mode from the latest snapshot inclusive.<br/>FROM_EARLIEST_SNAPSHOT: Start incremental mode from the earliest snapshot inclusive.<br/>FROM_SNAPSHOT_ID: Start incremental mode from a snapshot with a specific id inclusive.<br/>FROM_SNAPSHOT_TIMESTAMP: Start incremental mode from a snapshot with a specific timestamp inclusive. |
| common-options           |         | no       | -                    | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |

## Task Example

### Simple:

```hocon
env {
  execution.parallelism = 2
  job.mode = "BATCH"
}

source {
  Iceberg {
    schema {
      fields {
        f2 = "boolean"
        f1 = "bigint"
        f3 = "int"
        f4 = "bigint"
        f5 = "float"
        f6 = "double"
        f7 = "date"
        f9 = "timestamp"
        f10 = "timestamp"
        f11 = "string"
        f12 = "bytes"
        f13 = "bytes"
        f14 = "decimal(19,9)"
        f15 = "array<int>"
        f16 = "map<string, int>"
      }
    }
    catalog_name = "seatunnel"
    catalog_type = "hadoop"
    warehouse = "file:///tmp/seatunnel/iceberg/hadoop/"
    namespace = "database1"
    table = "source"
    result_table_name = "iceberg"
  }
}

transform {
}

sink {
  Console {
    source_table_name = "iceberg"
  }
}
```

### Hive Catalog:

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

### Column Projection:

```hocon
source {
  Iceberg {
    catalog_name = "seatunnel"
    catalog_type = "hadoop"
    warehouse = "hdfs://your_cluster/tmp/seatunnel/iceberg/"
    namespace = "your_iceberg_database"
    table = "your_iceberg_table"

    schema {
      fields {
        f2 = "boolean"
        f1 = "bigint"
        f3 = "int"
        f4 = "bigint"
      }
    }
  }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Iceberg Source Connector

### next version

- [Feature] Support Hadoop3.x ([3046](https://github.com/apache/seatunnel/pull/3046))
- [improve][api] Refactoring schema parse ([4157](https://github.com/apache/seatunnel/pull/4157))

