import ChangeLog from '../changelog/connector-iceberg.md';

# Apache Iceberg

> Apache Iceberg sink连接器

## Iceberg 版本支持

- 1.6.1

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

Apache Iceberg 目标连接器支持cdc模式、自动建表及表结构变更.

## 主要特性

- [x] [支持多表写入](../../concept/connector-v2-features.md)

## 支持的数据源信息

| 数据源     | 依赖项       | Maven依赖                                                             |
|---------|-----------|---------------------------------------------------------------------|
| Iceberg | hive-exec | [下载](https://mvnrepository.com/artifact/org.apache.hive/hive-exec)  |
| Iceberg | libfb303  | [下载](https://mvnrepository.com/artifact/org.apache.thrift/libfb303) |

## 数据库依赖

> 为了确保与不同版本的 Hadoop 和 Hive 兼容，项目 pom 文件中的 hive-exec 依赖范围被设置为 provided。因此，如果您使用 Flink 引擎，可能需要将以下 Jar 包添加到 <FLINK_HOME>/lib 目录中；如果您使用的是 Spark 引擎并且已经集成了 Hadoop，则无需添加以下 Jar 包。

```
hive-exec-xxx.jar
libfb303-xxx.jar
```

> 某些版本的 hive-exec 包中不包含 libfb303-xxx.jar，因此您还需要手动导入该 Jar 包。

## 数据类型映射

| SeaTunnel 数据类型 | Iceberg 数据类型     |
|----------------|------------------|
| BOOLEAN        | BOOLEAN          |
| INT            | INTEGER          |
| BIGINT         | LONG             |
| FLOAT          | FLOAT            |
| DOUBLE         | DOUBLE           |
| DATE           | DATE             |
| TIME           | TIME             |
| TIMESTAMP      | TIMESTAMP        |
| STRING         | STRING           |
| BYTES          | FIXED<br/>BINARY |
| DECIMAL        | DECIMAL          |
| ROW            | STRUCT           |
| ARRAY          | LIST             |
| MAP            | MAP              |

## Sink 选项

| 名称                                     | 类型      | 是否必须 | 默认                           | 描述                                                                                                                                                                                                                |
|----------------------------------------|---------|------|------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| catalog_name                           | string  | yes  | default                      | 用户指定的目录名称，默认为`default`                                                                                                                                                                                            |
| namespace                              | string  | yes  | default                      | backend catalog（元数据存储的后端目录）中 Iceberg 数据库的名称，默认为 `default`                                                                                                                                                         |
| table                                  | string  | yes  | -                            | backend catalog（元数据存储的后端目录）中 Iceberg 表的名称                                                                                                                                                                         |
| iceberg.catalog.config                 | map     | yes  | -                            | 用于指定初始化 Iceberg Catalog 的属性，这些属性可以参考此文件："https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/CatalogProperties.java"                                                                |
| hadoop.config                          | map     | no   | -                            | 传递给 Hadoop 配置的属性                                                                                                                                                                                                  |
| iceberg.hadoop-conf-path               | string  | no   | -                            | 指定`core-site.xml`、`hdfs-site.xml`、`hive-site.xml` 文件的加载路径                                                                                                                                                         |
| case_sensitive                         | boolean | no   | false                        | 列名匹配时是否区分大小写                                                                                                                                                                                                      |
| iceberg.table.write-props              | map     | no   | -                            | 传递给 Iceberg 写入器初始化的属性，这些属性具有最高优先级，例如 `write.format.default`、`write.target-file-size-bytes` 等设置。具体参数可以参考：'https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/TableProperties.java'. |
| iceberg.table.auto-create-props        | map     | no   | -                            | Iceberg 自动建表时指定的配置                                                                                                                                                                                                |
| iceberg.table.schema-evolution-enabled | boolean | no   | false                        | 设置为 true 时，Iceberg 表可以在同步过程中支持 schema 变更                                                                                                                                                                          |
| iceberg.table.primary-keys             | string  | no   | -                            | 用于标识表中一行数据的主键列列表，默认情况下以逗号分隔                                                                                                                                                                                       |
| iceberg.table.partition-keys           | string  | no   | -                            | 创建表时使用的分区字段列表，默认情况下以逗号分隔                                                                                                                                                                                          |
| iceberg.table.upsert-mode-enabled      | boolean | no   | false                        | 设置为 `true` 以启用 upsert 模式，默认值为 `false`                                                                                                                                                                             |
| schema_save_mode                       | Enum    | no   | CREATE_SCHEMA_WHEN_NOT_EXIST | schema 变更方式, 请参考下面的 `schema_save_mode`                                                                                                                                                                            |
| data_save_mode                         | Enum    | no   | APPEND_DATA                  | 数据写入方式, 请参考下面的 `data_save_mode`                                                                                                                                                                                   |
| custom_sql                             | string  | no   | -                            | 自定义 `delete` 数据的 SQL 语句，用于数据写入方式。例如： `delete from ... where ...`                                                                                                                                                  |
| iceberg.table.commit-branch            | string  | no   | -                            | 提交的默认分支                                                                                                                                                                                                           |

## 任务示例

### 简单示例

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

### Hadoop catalog

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

### AWS S3 Tables REST Catalog

Amazon S3 表类数据存储服务提供针对分析工作负载进行优化的 S3 存储，其功能旨在持续提高查询性能并降低表的存储成本。S3 表类数据存储服务专为存储表数据而设计，例如每日购买交易、流传感器数据或广告展示次数。表数据以列和行表示数据，就像在数据库表中一样。

您可以将 Iceberg REST 客户端连接到 Amazon S3 表类数据存储服务 Iceberg REST 端点，然后进行 REST API 调用来创建、更新或查询 S3 表存储桶中的表。该端点实现了 Apache Iceberg REST Catalog Open API specification 中指定的一组标准化 Iceberg REST API。该端点的工作原理是将 Iceberg REST API 操作转换为相应的 S3 表类数据存储服务操作。

S3 表类数据存储服务中的数据存储在新的存储桶类型中：表存储桶，它将表存储为子资源。表存储桶支持以 Apache Iceberg 格式存储表。使用标准 SQL 语句，您可以通过支持 Iceberg 的查询引擎来查询表，例如 Amazon Athena、Amazon Redshift 和 Apache Spark。

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

### Multiple table（多表写入）

#### 示例1

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

#### 示例2

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

## 变更日志

<ChangeLog />
