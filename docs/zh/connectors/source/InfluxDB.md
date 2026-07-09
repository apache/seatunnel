import ChangeLog from '../changelog/connector-influxdb.md';

# InfluxDB

> InfluxDB 源连接器

## 描述

通过 InfluxQL 查询从 InfluxDB 1.x 读取数据。连接器支持普通单查询，也支持按一个整数列范围切分查询，
让多个并行任务分别读取不同范围的数据。

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)

支持查询 SQL 并可以实现投影效果。

- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义 split](../../introduction/concepts/connector-v2-features.md)

## 选项

| 参数名                | 类型     | 必须 | 默认值   | 描述                                                                            |
|--------------------|--------|----|-------|-------------------------------------------------------------------------------|
| url                | string | 是  | -     | InfluxDB 连接 URL                                                               |
| sql                | string | 是  | -     | 用于搜索数据的查询 SQL                                                                 |
| schema             | config | 是  | -     | 上游数据的模式信息。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。 |
| database           | string | 是  | -     | InfluxDB 数据库                                                                  |
| username           | string | 否  | -     | InfluxDB 用户名。必须和 `password` 一起配置。                                                |
| password           | string | 否  | -     | InfluxDB 密码。必须和 `username` 一起配置。                                                |
| lower_bound        | int    | 否  | -     | 启用并行范围读取时，`split_column` 的下界。                                                   |
| upper_bound        | int    | 否  | -     | 启用并行范围读取时，`split_column` 的上界。                                                   |
| partition_num      | int    | 否  | 0     | 查询切分数量。`0` 表示不切分，直接执行原始 `sql`。                                              |
| split_column       | string | 否  | -     | 分割列                                                                           |
| where              | string | 否  | -     | 预留的 source 配置项。当前切分逻辑直接从 `sql` 中读取小写 `where` 关键字。                              |
| epoch              | string | 否  | n     | 返回的时间精度，例如：`H`、`m`、`s`、`MS`、`u`、`n`。                                     |
| connect_timeout_ms | long   | 否  | 15000 | 连接 InfluxDB 的超时时间（毫秒）                                                         |
| query_timeout_sec  | int    | 否  | 3     | 查询超时时间（秒）                                                                     |
| common-options     | config | 否  | -     | 源插件通用参数                                                                       |

### url

连接到 InfluxDB 的 URL，例如：

```
http://influxdb-host:8086
```

### sql [string]

用于搜索数据的查询 SQL

```
select name,age from test
```

### schema [config]

#### fields [Config]

上游数据的模式信息，例如：

```
schema {
    fields {
        name = string
        age = int
    }
  }
```

### database [string]

InfluxDB 数据库

### username [string]

InfluxDB 用户名

### password [string]

InfluxDB 密码

### split_column [string]

用于把一次查询切分成多个范围查询的列。

> 提示：
> - InfluxDB tags 不支持作为分割主键，因为 tags 的类型只能是字符串
> - InfluxDB time 不支持作为分割主键，因为 time 字段无法参与数学计算
> - 目前，`split_column` 仅支持整数数据分割，不支持 `float`、`string`、`date` 等类型。
> - `split_column`、`lower_bound`、`upper_bound`、`partition_num` 需要一起配置。
> - 如果切分读取的 SQL 中包含过滤条件，请在 `sql` 里使用小写 `where`，例如 `select * from test where age > 0`。当前切分解析逻辑区分大小写。

### upper_bound [int]

`split_column` 列的上界

### lower_bound [int]

`split_column` 列的下界

```
     将 $split_column 范围分成 $partition_num 部分
     如果 partition_num 为 1，使用整个 `split_column` 范围
     如果 partition_num < (upper_bound - lower_bound)，使用 (upper_bound - lower_bound) 个分区
     
     例如：lower_bound = 1, upper_bound = 10, partition_num = 2
     sql = "select * from test where age > 0 and age < 10"
     
     分割结果

     分割 1: select * from test where ($split_column >= 1 and $split_column < 6)  and (  age > 0 and age < 10 )
     
     分割 2: select * from test where ($split_column >= 6 and $split_column < 11) and (  age > 0 and age < 10 )

```

### partition_num [int]

InfluxDB 的分区数量

> 提示：确保 `upper_bound` 减去 `lower_bound` 能被 `partition_num` 整除，否则查询结果会重叠

### epoch [string]

返回的时间精度
- 可选值：H, m, s, MS, u, n
- 默认值：n

### query_timeout_sec [int]

InfluxDB 的查询超时时间（秒）

### connect_timeout_ms [long]

连接到 InfluxDB 的超时时间（毫秒）

### 通用选项

源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。

## 示例

### 使用并行范围读取

```hocon
env {
    parallelism = 1
    job.mode = "BATCH"
}

source {

    InfluxDB {
        url = "http://influxdb-host:8086"
        sql = "select label, c_string, c_double, c_bigint, c_float, c_int, c_smallint, c_boolean from source"
        database = "test"
        upper_bound = 99
        lower_bound = 0
        partition_num = 4
        split_column = "c_int"
        schema {
            fields {
                label = STRING
                c_string = STRING
                c_double = DOUBLE
                c_bigint = BIGINT
                c_float = FLOAT
                c_int = INT
                c_smallint = SMALLINT
                c_boolean = BOOLEAN
                time = BIGINT
            }
        }
    }

}

sink {
    Console {}
}
```

### 不使用并行范围读取

```hocon
env {
    parallelism = 1
    job.mode = "BATCH"
}

source {

    InfluxDB {
        url = "http://influxdb-host:8086"
        sql = "select label, c_string, c_double, c_bigint, c_float, c_int, c_smallint, c_boolean from source"
        database = "test"
        schema {
            fields {
                label = STRING
                c_string = STRING
                c_double = DOUBLE
                c_bigint = BIGINT
                c_float = FLOAT
                c_int = INT
                c_smallint = SMALLINT
                c_boolean = BOOLEAN
                time = BIGINT
            }
        }
    }

}

sink {
    Console {}
}
```

### 使用 InfluxQL 时区查询

```hocon
env {
    parallelism = 1
    job.mode = "BATCH"
}

source {
    InfluxDB {
        url = "http://influxdb-host:8086"
        sql = "select label, c_string, c_double, c_bigint, c_float, c_int, c_smallint, c_boolean from source tz('Asia/Shanghai')"
        database = "test"
        schema {
            fields {
                label = STRING
                c_string = STRING
                c_double = DOUBLE
                c_bigint = BIGINT
                c_float = FLOAT
                c_int = INT
                c_smallint = SMALLINT
                c_boolean = BOOLEAN
                time = BIGINT
            }
        }
    }
}

sink {
    Console {}
}
```

## 变更日志

<ChangeLog />
