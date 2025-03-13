# Apache Iceberg

> Apache Iceberg source connector

## Support Iceberg Version

- 1.4.2

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

| Datasource | Dependent |                                   Maven                                   |
|------------|-----------|---------------------------------------------------------------------------|
| Iceberg    | hive-exec | [Download](https://mvnrepository.com/artifact/org.apache.hive/hive-exec)  |
| Iceberg    | libfb303  | [Download](https://mvnrepository.com/artifact/org.apache.thrift/libfb303) |

## Database Dependency

> In order to be compatible with different versions of Hadoop and Hive, the scope of hive-exec in the project pom file are provided, so if you use the Flink engine, first you may need to add the following Jar packages to <FLINK_HOME>/lib directory, if you are using the Spark engine and integrated with Hadoop, then you do not need to add the following Jar packages. If you are using the hadoop s3 catalog, you need to add the hadoop-aws,aws-java-sdk jars for your Flink and Spark engine versions. (Additional locations: <FLINK_HOME>/lib, <SPARK_HOME>/jars)

```
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

| Name                     | Type    | Required | Default              | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|--------------------------|---------|----------|----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| catalog_name             | string  | yes      | -                    | User-specified catalog name.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| namespace                | string  | yes      | -                    | The iceberg database name in the backend catalog.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| table                    | string  | no       | -                    | The iceberg table name in the backend catalog.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| table_list               | string  | no       | -                    | The iceberg table list in the backend catalog.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| iceberg.catalog.config   | map     | yes      | -                    | Specify the properties for initializing the Iceberg catalog, which can be referenced in this file:"https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/CatalogProperties.java"                                                                                                                                                                                                                                                                                                                                                                                                           |
| hadoop.config            | map     | no       | -                    | Properties passed through to the Hadoop configuration                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| iceberg.hadoop-conf-path | string  | no       | -                    | The specified loading paths for the 'core-site.xml', 'hdfs-site.xml', 'hive-site.xml' files.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| schema                   | config  | no       | -                    | Use projection to select data columns and columns order.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| case_sensitive           | boolean | no       | false                | If data columns where selected via schema [config], controls whether the match to the schema will be done with case sensitivity.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| start_snapshot_timestamp | long    | no       | -                    | Instructs this scan to look for changes starting from  the most recent snapshot for the table as of the timestamp. <br/>timestamp – the timestamp in millis since the Unix epoch                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| start_snapshot_id        | long    | no       | -                    | Instructs this scan to look for changes starting from a particular snapshot (exclusive).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| end_snapshot_id          | long    | no       | -                    | Instructs this scan to look for changes up to a particular snapshot (inclusive).                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| use_snapshot_id          | long    | no       | -                    | Instructs this scan to look for use the given snapshot ID.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| use_snapshot_timestamp   | long    | no       | -                    | Instructs this scan to look for use the most recent snapshot as of the given time in milliseconds. timestamp – the timestamp in millis since the Unix epoch                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
| stream_scan_strategy     | enum    | no       | FROM_LATEST_SNAPSHOT | Starting strategy for stream mode execution, Default to use `FROM_LATEST_SNAPSHOT` if don’t specify any value,The optional values are:<br/>TABLE_SCAN_THEN_INCREMENTAL: Do a regular table scan then switch to the incremental mode.<br/>FROM_LATEST_SNAPSHOT: Start incremental mode from the latest snapshot inclusive.<br/>FROM_EARLIEST_SNAPSHOT: Start incremental mode from the earliest snapshot inclusive.<br/>FROM_SNAPSHOT_ID: Start incremental mode from a snapshot with a specific id inclusive.<br/>FROM_SNAPSHOT_TIMESTAMP: Start incremental mode from a snapshot with a specific timestamp inclusive. |
| increment.scan-interval  | long    | no       | 2000                 | The interval of increment scan(mills)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| common-options           |         | no       | -                    | Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |

## Task Example

### Simple:

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Iceberg {
    catalog_name = "seatunnel"
    iceberg.catalog.config={
      type = "hadoop"
      warehouse = "file:///tmp/seatunnel/iceberg/hadoop/"
    }
    namespace = "database1"
    table = "source"
    plugin_output = "iceberg"
  }
}

transform {
}

sink {
  Console {
    plugin_input = "iceberg"
  }
}
```

### Multi-Table Read:

```hocon
source {
  Iceberg {
    catalog_name = "seatunnel"
    iceberg.catalog.config = {
      type = "hadoop"
      warehouse = "file:///tmp/seatunnel/iceberg/hadoop/"
    }
    namespace = "database1"
    table_list = [
      {
        table = "table_1
      },
      {
        table = "table_2
      }
    ]
    
    plugin_output = "iceberg"
  }
}
```

### Hadoop S3 Catalog:

```hocon
source {
  iceberg {
    catalog_name = "seatunnel"
    iceberg.catalog.config={
      "type"="hadoop"
      "warehouse"="s3a://your_bucket/spark/warehouse/"
    }
    hadoop.config={
      "fs.s3a.aws.credentials.provider" = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
      "fs.s3a.endpoint" = "s3.cn-north-1.amazonaws.com.cn"
      "fs.s3a.access.key" = "xxxxxxxxxxxxxxxxx"
      "fs.s3a.secret.key" = "xxxxxxxxxxxxxxxxx"
      "fs.defaultFS" = "s3a://your_bucket"
    }
    namespace = "your_iceberg_database"
    table = "your_iceberg_table"
    plugin_output = "iceberg_test"
  }
}
```

### Hive Catalog:

```hocon
source {
  Iceberg {
    catalog_name = "seatunnel"
    iceberg.catalog.config={
      type = "hive"
      uri = "thrift://localhost:9083"
      warehouse = "hdfs://your_cluster//tmp/seatunnel/iceberg/"
    }
    catalog_type = "hive"
    
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
    iceberg.catalog.config={
      type = "hadoop"
      warehouse = "hdfs://your_cluster/tmp/seatunnel/iceberg/"
    }
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

