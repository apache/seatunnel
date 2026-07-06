import ChangeLog from '../changelog/connector-selectdb-cloud.md';

# SelectDB Cloud

> SelectDB Cloud Sink 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 描述

用于将数据发送到 SelectDB Cloud。支持流式和批处理模式。

SelectDB Cloud 接收器连接器的内部实现是在批量缓存后上传数据，并提交 CopyInto SQL 以将数据加载到表中。

## 支持的数据源信息

:::提示

支持的版本

* 支持的 `SelectDB Cloud 版本 >= 2.2.x`

:::

## 接收器选项

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
|------|------|----------|--------|------|
| load-url | String | 是 | - | SelectDB Cloud 仓库 HTTP 地址，格式为 `warehouse_ip:http_port`。 |
| jdbc-url | String | 是 | - | SelectDB Cloud 仓库 JDBC 地址，格式为 `warehouse_ip:mysql_port`。 |
| cluster-name | String | 是 | - | SelectDB Cloud 集群名称。 |
| username | String | 是 | - | SelectDB Cloud 用户名。 |
| password | String | 否 | - | SelectDB Cloud 用户密码。 |
| table.identifier | String | 是 | - | SelectDB Cloud 表名，格式为 `database.table`。 |
| sink.enable-2pc | Boolean | 否 | true | 是否启用两阶段提交。开启后，连接器会通过 checkpoint 提交路径提供精确一次写入语义。 |
| sink.enable-delete | Boolean | 否 | false | 是否写入删除事件。目标 SelectDB Cloud 表必须启用批量删除，并且使用 Unique 模型。 |
| sink.max-retries | Int | 否 | 3 | 写入失败时的最大重试次数。 |
| sink.buffer-size | Int | 否 | 10485760 | 上传缓存数据前的缓冲区大小，单位为字节。默认值为 10 MB。 |
| sink.buffer-count | Int | 否 | 10000 | 上传数据前缓存的行数。 |
| sink.label-prefix | String | 否 | 随机 UUID | Load 任务使用的唯一 label 前缀。如果需要更容易追踪 load label，可以配置固定值。 |
| sink.flush.queue-size | Int | 否 | 1 | 异步上传到对象存储的队列长度。 |
| selectdb.config | Map | 否 | - | 额外的 Copy Into data description 参数。配置时需要在原始 load 参数名前加 `selectdb.config` 前缀，例如 `selectdb.config.file.type = "json"`。 |

### CDC 和精确一次说明

SelectDB Cloud Sink 可以消费插入、更新和删除类型的数据。删除事件只有在 `sink.enable-delete = true` 时才会生效，并且目标表需要满足 SelectDB Cloud 的删除写入要求。

`sink.enable-2pc = true` 是默认值，也是推荐的精确一次写入配置。如果一次大批量写入导致缓存文件保留时间超过 SelectDB Cloud 的过期窗口，可以将 `sink.enable-2pc` 设置为 `false`，但这时写入语义会降级为至少一次。

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

支持的格式包括 CSV 和 JSON。格式通过 `selectdb.config` 配置，例如 `selectdb.config.file.type = "json"`。

## 任务示例

### 简单示例

> 以下示例描述了将多种数据类型写入 SelectDBCloud，用户需要在下游创建相应的表

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval = 10000
}

source {
  Jdbc {
    driver = com.mysql.cj.jdbc.Driver
    url = "jdbc:mysql://selectdb_e2e:9030"
    username = admin
    password = ""
    query = "select BIGINT_COL, LARGEINT_COL, SMALLINT_COL, TINYINT_COL, BOOLEAN_COL, DECIMAL_COL, DOUBLE_COL, FLOAT_COL, INT_COL, CHAR_COL, VARCHAR_11_COL, STRING_COL, DATETIME_COL, DATE_COL from `test`.`e2e_table_source`"
  }
}

sink {
  SelectDBCloud {
    load-url = "warehouse_ip:http_port"
    jdbc-url = "warehouse_ip:mysql_port"
    cluster-name = "Cluster"
    table.identifier = "test.e2e_table_sink"
    username = "admin"
    password = "******"
    sink.enable-2pc = true
    selectdb.config {
      file.type = "json"
      file.strip_outer_array = "false"
    }
  }
}
```

### 写入 FakeSource 数据

```hocon
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

## 变更日志

<ChangeLog />
