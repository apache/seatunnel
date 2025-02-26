# SelectDB Cloud

> SelectDB Cloud Sink 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

## 描述

用于将数据发送到 SelectDB Cloud。支持流式和批处理模式。

SelectDB Cloud 接收器连接器的内部实现是在批量缓存后上传数据，并提交 CopyInto SQL 以将数据加载到表中。

## 支持的数据源信息

:::提示

支持的版本

* 支持的 `SelectDB Cloud 版本 >= 2.2.x`

:::

## 接收器选项

|        名称        |  类型  | 是否必填 |        默认值         |                                                                                                                                                                    描述                                                                                                                                                                    |
|--------------------|--------|----------|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| load-url           | String | 是       | -                      | `SelectDB Cloud` 仓库的 HTTP 地址，格式为 `warehouse_ip:http_port`                                                                                                                                                                                                                                                                          |
| jdbc-url           | String | 是       | -                      | `SelectDB Cloud` 仓库的 JDBC 地址，格式为 `warehouse_ip:mysql_port`                                                                                                                                                                                                                                                                         |
| cluster-name       | String | 是       | -                      | `SelectDB Cloud` 集群名称                                                                                                                                                                                                                                                                                                                  |
| username           | String | 是       | -                      | `SelectDB Cloud` 用户名                                                                                                                                                                                                                                                                                                                    |
| password           | String | 是       | -                      | `SelectDB Cloud` 用户密码                                                                                                                                                                                                                                                                                                                  |
| sink.enable-2pc    | bool   | 否       | true                   | 是否启用两阶段提交（2pc），默认为 true，以确保 Exactly-Once 语义。SelectDB 使用缓存文件加载数据。当数据量较大时，缓存数据可能会失效（默认过期时间为 1 小时）。如果遇到大量数据写入丢失的情况，请将 sink.enable-2pc 配置为 false。                                                                                                           |
| table.identifier   | String | 是       | -                      | `SelectDB Cloud` 表的名称，格式为 `database.table`                                                                                                                                                                                                                                                                                          |
| sink.enable-delete | bool   | 否       | false                  | 是否启用删除功能。此选项要求 SelectDB Cloud 表启用批量删除功能，并且仅支持 Unique 模型。                                                                                                                                                                                                                                                     |
| sink.max-retries   | int   | 否       | 3                      | 写入数据库失败时的最大重试次数                                                                                                                                                                                                                                                                                                             |
| sink.buffer-size   | int   | 否       | 10 * 1024 * 1024 (1MB) | 用于流式加载的数据缓存缓冲区大小                                                                                                                                                                                                                                                                                                           |
| sink.buffer-count  | int   | 否       | 10000                  | 用于流式加载的数据缓存缓冲区数量                                                                                                                                                                                                                                                                                                           |
| selectdb.config    | map   | 是       | -                      | 此选项用于在自动生成 SQL 时支持 `insert`、`delete` 和 `update` 等操作，并支持多种格式。                                                                                                                                                                                                                                                     |

## 数据类型映射

| SelectDB Cloud 数据类型 |           SeaTunnel 数据类型           |
|--------------------------|-----------------------------------------|
| BOOLEAN                  | BOOLEAN                                 |
| TINYINT                  | TINYINT                                 |
| SMALLINT                 | SMALLINT<br/>TINYINT                    |
| INT                      | INT<br/>SMALLINT<br/>TINYINT            |
| BIGINT                   | BIGINT<br/>INT<br/>SMALLINT<br/>TINYINT |
| LARGEINT                 | BIGINT<br/>INT<br/>SMALLINT<br/>TINYINT |
| FLOAT                    | FLOAT                                   |
| DOUBLE                   | DOUBLE<br/>FLOAT                        |
| DECIMAL                  | DECIMAL<br/>DOUBLE<br/>FLOAT            |
| DATE                     | DATE                                    |
| DATETIME                 | TIMESTAMP                               |
| CHAR                     | STRING                                  |
| VARCHAR                  | STRING                                  |
| STRING                   | STRING                                  |
| ARRAY                    | ARRAY                                   |
| MAP                      | MAP                                     |
| JSON                     | STRING                                  |
| HLL                      | 尚未支持                                |
| BITMAP                   | 尚未支持                                |
| QUANTILE_STATE           | 尚未支持                                |
| STRUCT                   | 尚未支持                                |

#### 支持的导入数据格式

支持的格式包括 CSV 和 JSON

## 任务示例

### 简单示例：

> 以下示例描述了将多种数据类型写入 SelectDBCloud，用户需要在下游创建相应的表

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval = 10000
}

source {
  FakeSource {
    row.num = 10
    map.size = 10
    array.size = 10
    bytes.length = 10
    string.length = 10
    schema = {
      fields {
        c_map = "map<string, array<int>>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(16, 1)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
    }
}

sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.test"
    username = "admin"
    password = "******"
    selectdb.config {
        file.type = "json"
    }
  }
}
```

### 使用 JSON 格式导入数据

```
sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.test"
    username = "admin"
    password = "******"
    selectdb.config {
        file.type = "json"
    }
  }
}

```

### 使用 CSV 格式导入数据

```
sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.test"
    username = "admin"
    password = "******"
    selectdb.config {
        file.type = "csv"
        file.column_separator = "," 
        file.line_delimiter = "\n" 
    }
  }
}
```
