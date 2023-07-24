# Apache Iceberg

> Apache Iceberg sink connector

## Description

Sink connector for Apache Iceberg. It only can support batch mode at present.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] data format
  - [x] parquet
  - [x] orc
  - [x] avro
- [x] iceberg catalog
  - [x] hadoop(2.7.1 , 2.7.5 , 3.1.3)
  - [x] hive(2.3.9 , 3.1.2)

## Options

|      name      |  type   | required | default value |
|----------------|---------|----------|---------------|
| catalog_name   | string  | yes      | -             |
| catalog_type   | string  | yes      | -             |
| uri            | string  | no       | -             |
| warehouse      | string  | yes      | -             |
| namespace      | string  | yes      | -             |
| table          | string  | yes      | -             |
| batch_size     | int     | no       | 1             |
| case_sensitive | boolean | no       | false         |
| common-options |         | no       | -             |

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

### batch_size [int]

For batch writing, when the number of buffered records reaches the number of `batch_size`, the data will be flushed into the database.

### table [string]

The iceberg table name in the backend catalog.

### case_sensitive [boolean]

If data columns where selected via schema [config], controls whether the match to the schema will be done with case sensitivity.

### common options

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

## Example

simple

```hocon
sink {
  Iceberg {
    catalog_name = "seatunnel"
    catalog_type = "hadoop"
    warehouse = "hdfs://your_cluster//tmp/seatunnel/iceberg/"
    namespace = "your_iceberg_database"
    table = "your_iceberg_table"
    batch_size = 3000
  }
}
```

Or

```hocon
sink {
  Iceberg {
    catalog_name = "seatunnel"
    catalog_type = "hive"
    uri = "thrift://localhost:9083"
    warehouse = "hdfs://your_cluster//tmp/seatunnel/iceberg/"
    namespace = "your_iceberg_database"
    table = "your_iceberg_table"
    batch_size = 3000
  }
}
```

:::tip

In order to be compatible with different versions of Hadoop and Hive, the scope of hive-exec and flink-shaded-hadoop-2 in the project pom file are provided, so if you use the Flink engine, first you may need to add the following Jar packages to <FLINK_HOME>/lib directory, if you are using the Spark engine and integrated with Hadoop, then you do not need to add the following Jar packages.

:::

```
flink-shaded-hadoop-x-xxx.jar
hive-exec-xxx.jar
libfb303-xxx.jar
```

Some versions of the hive-exec package do not have libfb303-xxx.jar, so you also need to manually import the Jar package.

## Changelog

### 2.3.2 2023-07-18

- Add Iceberg Sink Connector

