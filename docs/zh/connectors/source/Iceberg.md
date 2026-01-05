import ChangeLog from '../changelog/connector-iceberg.md';

# Apache Iceberg

> Apache Iceberg 源连接器

## 支持 Iceberg 版本

- 1.6.1

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [x] [流](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)
- [x] 数据格式
  - [x] parquet
  - [x] orc
  - [x] avro
- [x] iceberg 目录
  - [x] hadoop(2.7.1 , 2.7.5 , 3.1.3)
  - [x] hive(2.3.9 , 3.1.2)

## 描述

Apache Iceberg 的源连接器。它可以支持批处理和流模式。

## 支持的数据源信息

| 数据源 | 依赖 |                                   Maven                                   |
|--------|------|---------------------------------------------------------------------------|
| Iceberg    | hive-exec | [下载](https://mvnrepository.com/artifact/org.apache.hive/hive-exec)  |
| Iceberg    | libfb303  | [下载](https://mvnrepository.com/artifact/org.apache.thrift/libfb303) |

## 数据库依赖

> 为了与不同版本的 Hadoop 和 Hive 兼容，项目 pom 文件中 hive-exec 的范围是 provided，所以如果您使用 Flink 引擎，首先您可能需要将以下 Jar 包添加到 <FLINK_HOME>/lib 目录，如果您使用 Spark 引擎并与 Hadoop 集成，则不需要添加以下 Jar 包。如果您使用 hadoop s3 目录，您需要为您的 Flink 和 Spark 引擎版本添加 hadoop-aws、aws-java-sdk jars。（其他位置：<FLINK_HOME>/lib、<SPARK_HOME>/jars）

```
hive-exec-xxx.jar
libfb303-xxx.jar
```

> hive-exec 包的某些版本没有 libfb303-xxx.jar，所以您还需要手动导入 Jar 包。

## 数据类型映射

| Iceberg 数据类型 | SeaTunnel 数据类型 |
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

## 源选项

| 参数名                     | 类型    | 必须 | 默认值              | 描述                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |
|--------------------------|---------|------|----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| catalog_name             | string  | 是   | -                    | 用户指定的目录名称。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| namespace                | string  | 是   | -                    | 后端目录中的 iceberg 数据库名称。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| table                    | string  | 否   | -                    | 后端目录中的 iceberg 表名称。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| table_list               | string  | 否   | -                    | 后端目录中的 iceberg 表列表。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| iceberg.catalog.config   | map     | 是   | -                    | 指定初始化 Iceberg 目录的属性，可以在此文件中引用：https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/CatalogProperties.java                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| hadoop.config            | map     | 否   | -                    | 传递给 Hadoop 配置的属性                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| iceberg.hadoop-conf-path | string  | 否   | -                    | 为 'core-site.xml'、'hdfs-site.xml'、'hive-site.xml' 文件指定的加载路径。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           |
| schema                   | config  | 否   | -                    | 使用投影来选择数据列和列顺序。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| case_sensitive           | boolean | 否   | false                | 如果通过 schema [config] 选择了数据列，控制是否将与 schema 的匹配进行区分大小写。                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| start_snapshot_timestamp | long    | 否   | -                    | 指示此扫描从表的最新快照开始查找更改，从给定的时间戳开始。<br/>timestamp – 自 Unix 纪元以来的时间戳（毫秒）                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| start_snapshot_id        | long    | 否   | -                    | 指示此扫描从特定快照（独占）开始查找更改。                                                                                                                                                                                                                                                                                                                                                                                                                               |
| end_snapshot_id          | long    | 否   | -                    | 指示此扫描查找更改直到特定快照（包含）。                                                                                                                                                                                                                                                                                                                                                                                                                                       |
| use_snapshot_id          | long    | 否   | -                    | 指示此扫描使用给定的快照 ID。                                                                                                                                                                                                                                                                                                                                                                                                                                             |
| use_snapshot_timestamp   | long    | 否   | -                    | 指示此扫描使用给定时间（毫秒）的最新快照。timestamp – 自 Unix 纪元以来的时间戳（毫秒）                                                                                                                                                                                                                                                                                                                                                            |
| stream_scan_strategy     | enum    | 否   | FROM_LATEST_SNAPSHOT | 流模式执行的启动策略，如果不指定任何值，默认使用 `FROM_LATEST_SNAPSHOT`，可选值为：<br/>TABLE_SCAN_THEN_INCREMENTAL：执行常规表扫描，然后切换到增量模式。<br/>FROM_LATEST_SNAPSHOT：从最新快照（包含）开始增量模式。<br/>FROM_EARLIEST_SNAPSHOT：从最早快照（包含）开始增量模式。<br/>FROM_SNAPSHOT_ID：从具有特定 id（包含）的快照开始增量模式。<br/>FROM_SNAPSHOT_TIMESTAMP：从具有特定时间戳（包含）的快照开始增量模式。 |
| increment.scan-interval  | long    | 否   | 2000                 | 增量扫描的间隔（毫秒）                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| common-options           |         | 否   | -                    | 源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| query                    | String  | 否   | -                    | 用于选择 iceberg 数据的 select DML。它不能包含表名，也不支持别名。例如：`select * from table where f1 > 100`、`select fn from table where f1 > 100`。当前对 LIKE 语法的支持是有限的：LIKE 子句不应以 `%` 开头。支持的是：`select f1 from t where f2 like 'tom%'  `                                                                                                                                                                                                                                                       |


## 任务示例

### 简单

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
    query = "select fn from table where f1 > 100"
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

### 多表读取

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
        table = "table_1"
      },
      {
        table = "table_2"
        query = "select fn from table where f1 > 100"
      }
    ]

    plugin_output = "iceberg"
  }
}
```

### Hadoop S3 目录

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

### Hive 目录

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

### 列投影

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

## 变更日志

<ChangeLog />


