import ChangeLog from '../changelog/connector-iceberg.md';

# Apache Iceberg

> Apache Iceberg sink connector

## Support Iceberg Version

- 1.6.1

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Sink connector for Apache Iceberg. It can support cdc mode 、auto create table and table schema evolution.

## Key features

- [x] [support multiple table write](../../concept/connector-v2-features.md)

## Supported DataSource Info

| Datasource | Dependent |                                   Maven                                   |
|------------|-----------|---------------------------------------------------------------------------|
| Iceberg    | hive-exec | [Download](https://mvnrepository.com/artifact/org.apache.hive/hive-exec)  |
| Iceberg    | libfb303  | [Download](https://mvnrepository.com/artifact/org.apache.thrift/libfb303) |

## Database Dependency

> In order to be compatible with different versions of Hadoop and Hive, the scope of hive-exec in the project pom file are provided, so if you use the Flink engine, first you may need to add the following Jar packages to <FLINK_HOME>/lib directory, if you are using the Spark engine and integrated with Hadoop, then you do not need to add the following Jar packages.

```
hive-exec-xxx.jar
libfb303-xxx.jar
```

> Some versions of the hive-exec package do not have libfb303-xxx.jar, so you also need to manually import the Jar package.

## Data Type Mapping

| SeaTunnel Data type | Iceberg Data type |
|---------------------|-------------------|
| BOOLEAN             | BOOLEAN           |
| INT                 | INTEGER           |
| BIGINT              | LONG              |
| FLOAT               | FLOAT             |
| DOUBLE              | DOUBLE            |
| DATE                | DATE              |
| TIME                | TIME              |
| TIMESTAMP           | TIMESTAMP         |
| STRING              | STRING            |
| BYTES               | FIXED<br/>BINARY  |
| DECIMAL             | DECIMAL           |
| ROW                 | STRUCT            |
| ARRAY               | LIST              |
| MAP                 | MAP               |

## Sink Options

| Name                                   | Type    | Required | Default                      | Description                                                                                                                                                                                                                                                                                                               |
|----------------------------------------|---------|----------|------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| catalog_name                           | string  | yes      | default                      | User-specified catalog name. default is `default`                                                                                                                                                                                                                                                                         |
| namespace                              | string  | yes      | default                      | The iceberg database name in the backend catalog. default is `default`                                                                                                                                                                                                                                                    |
| table                                  | string  | yes      | -                            | The iceberg table name in the backend catalog.                                                                                                                                                                                                                                                                            |
| iceberg.catalog.config                 | map     | yes      | -                            | Specify the properties for initializing the Iceberg catalog, which can be referenced in this file:"https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/CatalogProperties.java"                                                                                                              |
| hadoop.config                          | map     | no       | -                            | Properties passed through to the Hadoop configuration                                                                                                                                                                                                                                                                     |
| iceberg.hadoop-conf-path               | string  | no       | -                            | The specified loading paths for the 'core-site.xml', 'hdfs-site.xml', 'hive-site.xml' files.                                                                                                                                                                                                                              |
| case_sensitive                         | boolean | no       | false                        | If data columns where selected via schema [config], controls whether the match to the schema will be done with case sensitivity.                                                                                                                                                                                          |
| iceberg.table.write-props              | map     | no       | -                            | Properties passed through to Iceberg writer initialization, these take precedence, such as 'write.format.default', 'write.target-file-size-bytes', and other settings, can be found with specific parameters at 'https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/TableProperties.java'. |
| iceberg.table.auto-create-props        | map     | no       | -                            | Configuration specified by Iceberg during automatic table creation.                                                                                                                                                                                                                                                       |
| iceberg.table.schema-evolution-enabled | boolean | no       | false                        | Setting to true enables Iceberg tables to support schema evolution during the synchronization process                                                                                                                                                                                                                     |
| iceberg.table.primary-keys             | string  | no       | -                            | Default comma-separated list of columns that identify a row in tables (primary key)                                                                                                                                                                                                                                       |
| iceberg.table.partition-keys           | string  | no       | -                            | Default comma-separated list of partition fields to use when creating tables                                                                                                                                                                                                                                              |
| iceberg.table.upsert-mode-enabled      | boolean | no       | false                        | Set to `true` to enable upsert mode, default is `false`                                                                                                                                                                                                                                                                   |
| schema_save_mode                       | Enum    | no       | CREATE_SCHEMA_WHEN_NOT_EXIST | the schema save mode, please refer to `schema_save_mode` below                                                                                                                                                                                                                                                            |
| data_save_mode                         | Enum    | no       | APPEND_DATA                  | the data save mode, please refer to `data_save_mode` below                                                                                                                                                                                                                                                                |
| custom_sql                             | string  | no       | -                            | Custom `delete` data sql for data save mode. e.g: `delete from ... where ...`                                                                                                                                                                                                                                             |
| iceberg.table.commit-branch            | string  | no       | -                            | Default branch for commits                                                                                                                                                                                                                                                                                                |

## Task Example

### Simple

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    plugin_output = "customers_mysql_cdc_iceberg"
    server-id = 5652
    username = "st_user"
    password = "seatunnel"
    table-names = ["mysql_cdc.mysql_cdc_e2e_source_table"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/mysql_cdc"
  }
}

transform {
}

sink {
  Iceberg {
    catalog_name="seatunnel_test"
    iceberg.catalog.config={
      "type"="hadoop"
      "warehouse"="file:///tmp/seatunnel/iceberg/hadoop-sink/"
    }
    namespace="seatunnel_namespace"
    table="iceberg_sink_table"
    iceberg.table.write-props={
      write.format.default="parquet"
      write.target-file-size-bytes=536870912
    }
    iceberg.table.primary-keys="id"
    iceberg.table.partition-keys="f_datetime"
    iceberg.table.upsert-mode-enabled=true
    iceberg.table.schema-evolution-enabled=true
    case_sensitive=true
  }
}
```

### Hive Catalog

```hocon
sink {
  Iceberg {
    catalog_name = "seatunnel_test"
    iceberg.catalog.config = {
      type = "hive"
      uri = "thrift://localhost:9083"
      warehouse = "hdfs://your_cluster/tmp/seatunnel/iceberg/"
    }
    namespace = "seatunnel_namespace"
    table = "iceberg_sink_table"
    iceberg.table.write-props = {
      write.format.default = "parquet"
      write.target-file-size-bytes = 536870912
    }
    iceberg.table.primary-keys = "id"
    iceberg.table.partition-keys = "f_datetime"
    iceberg.table.upsert-mode-enabled = true
    iceberg.table.schema-evolution-enabled = true
    case_sensitive = true
  }
}
```

### Hadoop Catalog

```hocon
sink {
  Iceberg {
    catalog_name = "seatunnel_test"
    iceberg.catalog.config = {
      type = "hadoop"
      warehouse = "hdfs://your_cluster/tmp/seatunnel/iceberg/"
    }
    namespace = "seatunnel_namespace"
    table = "iceberg_sink_table"
    iceberg.table.write-props = {
      write.format.default = "parquet"
      write.target-file-size-bytes = 536870912
    }
    iceberg.table.primary-keys = "id"
    iceberg.table.partition-keys = "f_datetime"
    iceberg.table.upsert-mode-enabled = true
    iceberg.table.schema-evolution-enabled = true
    case_sensitive = true
  }
}
```

### Glue Catalog

```hocon
sink {
  Iceberg {
    catalog_name = "seatunnel_test"
    iceberg.catalog.config = {
      warehouse     = "s3://your-bucket/warehouse/"
      catalog-impl  = "org.apache.iceberg.aws.glue.GlueCatalog"
      io-impl       = "org.apache.iceberg.aws.s3.S3FileIO"
      client.region = "your-region"
    }
    namespace = "seatunnel_namespace"
    table     = "iceberg_sink_table"
    iceberg.table.write-props = {
      write.format.default = "parquet"
      write.target-file-size-bytes = 536870912
    }
    iceberg.table.primary-keys = "id"
    iceberg.table.partition-keys = "f_datetime"
    iceberg.table.upsert-mode-enabled = true
    iceberg.table.schema-evolution-enabled = true
    case_sensitive = true
  }
}

```

### AWS S3 Tables REST Catalog

Amazon S3 Tables is a storage service for tabular data that's optimized for analytics workloads, with features designed to continuously improve query performance and reduce storage costs for tables. S3 Tables is purpose-built for storing tabular data, such as daily purchase transactions, streaming sensor data, or ad impressions. Tabular data represents data in columns and rows, like in a database table.

You can connect an Iceberg REST client to the Amazon S3 Tables Iceberg REST endpoint and then make REST API calls to create, update, or query tables in S3 table buckets. The endpoint implements a standardized set of Iceberg REST APIs specified in the Apache Iceberg REST Catalog Open API specification. The endpoint works by translating Iceberg REST API operations to corresponding S3 Tables operations.

Data in S3 Tables is stored in a new bucket type: table buckets, which store tables as subresources. Table buckets support storing tables in Apache Iceberg format. Using standard SQL statements, you can query tables through Iceberg-compatible query engines such as Amazon Athena, Amazon Redshift, and Apache Spark.

```hocon
sink {
  Iceberg {
    catalog_name = "s3_tables_catalog"
    namespace = "s3_tables_catalog"
    table = "user_data"

    iceberg.catalog.config = {
      type: "rest"
      warehouse: "arn:aws:s3tables:<Region>:<accountID>:bucket/<bucketname>"
      uri: "https://s3tables.<Region>.amazonaws.com/iceberg"
      rest.sigv4-enabled: "true"
      rest.signing-name: "s3tables"
      rest.signing-region: "<Region>"
    }
  }
}
```

### Multiple table

#### example1

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    
    table-names = ["seatunnel.role","seatunnel.user","galileo.Bucket"]
  }
}

transform {
}

sink {
  Iceberg {
    ...
    namespace = "${database_name}_test"
    table = "${table_name}_test"
  }
}
```

#### example2

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = oracle.jdbc.driver.OracleDriver
    url = "jdbc:oracle:thin:@localhost:1521/XE"
    user = testUser
    password = testPassword

    table_list = [
      {
        table_path = "TESTSCHEMA.TABLE_1"
      },
      {
        table_path = "TESTSCHEMA.TABLE_2"
      }
    ]
  }
}

transform {
}

sink {
  Iceberg {
    ...
    namespace = "${schema_name}_test"
    table = "${table_name}_test"
  }
}
```

## Changelog

<ChangeLog />
