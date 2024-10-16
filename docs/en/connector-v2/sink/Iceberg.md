# Apache Iceberg

> Apache Iceberg sink connector

## Support Iceberg Version

- 1.4.2

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

|                  Name                  |  Type   | Required |           Default            |                                                                                                                                                        Description                                                                                                                                                        |
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
| iceberg.table.commit-branch            | string  | no       | -                            | Default branch for commits                                                                                                                                                                                                                                                                                                |

## Task Example

### Simple:

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    result_table_name = "customers_mysql_cdc_iceberg"
    server-id = 5652
    username = "st_user"
    password = "seatunnel"
    table-names = ["mysql_cdc.mysql_cdc_e2e_source_table"]
    base-url = "jdbc:mysql://mysql_cdc_e2e:3306/mysql_cdc"
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

### Hive Catalog:

```hocon
sink {
  Iceberg {
    catalog_name="seatunnel_test"
    iceberg.catalog.config={
      type = "hive"
      uri = "thrift://localhost:9083"
      warehouse = "hdfs://your_cluster//tmp/seatunnel/iceberg/"
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

### Hadoop catalog:

```hocon
sink {
  Iceberg {
    catalog_name="seatunnel_test"
    iceberg.catalog.config={
      type = "hadoop"
      warehouse = "hdfs://your_cluster/tmp/seatunnel/iceberg/"
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
    base-url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
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

### 2.3.4-SNAPSHOT 2024-01-18

- Add Iceberg Sink Connector

### next version

