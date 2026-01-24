import ChangeLog from '../changelog/connector-influxdb.md';

# InfluxDB

> InfluxDB 源连接器

## 描述

通过 InfluxDB 读取外部数据源数据。

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)

支持查询 SQL 并可以实现投影效果。

- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义 split](../../introduction/concepts/connector-v2-features.md)

## 选项

| 参数名                | 类型     | 必须 | 默认值   | 描述                                                                            |
|--------------------|--------|----|-------|-------------------------------------------------------------------------------|
| url                | string | 是  | -     | InfluxDB 连接 URL                                                               |
| sql                | string | 是  | -     | 用于搜索数据的查询 SQL                                                                 |
| schema             | config | 是  | -     | 上游数据的模式信息。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。 |
| database           | string | 是  | -     | InfluxDB 数据库                                                                  |
| username           | string | 否  | -     | InfluxDB 用户名                                                                  |
| password           | string | 否  | -     | InfluxDB 密码                                                                   |
| lower_bound        | long   | 否  | -     | split_column 的下界                                                              |
| upper_bound        | long   | 否  | -     | split_column 的上界                                                              |
| partition_num      | int    | 否  | -     | 分区数量                                                                          |
| split_column       | string | 否  | -     | 分割列                                                                           |
| epoch              | string | 否  | n     | 返回的时间精度                                                                       |
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

InfluxDB 的分割列

> 提示：
> - InfluxDB tags 不支持作为分割主键，因为 tags 的类型只能是字符串
> - InfluxDB time 不支持作为分割主键，因为 time 字段无法参与数学计算
> - 目前，`split_column` 仅支持整数数据分割，不支持 `float`、`string`、`date` 等类型。

### upper_bound [long]

`split_column` 列的上界

### lower_bound [long]

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

多并行性和多分区扫描示例

```hocon
source {

    InfluxDB {
        url = "http://influxdb-host:8086"
        sql = "select label, value, rt, time from test"
        database = "test"
        upper_bound = 100
        lower_bound = 1
        partition_num = 4
        split_column = "value"
        schema {
            fields {
                label = STRING
                value = INT
                rt = STRING
                time = BIGINT
            }
        }
    }

}

```

不使用分区扫描的示例

```hocon
source {

    InfluxDB {
        url = "http://influxdb-host:8086"
        sql = "select label, value, rt, time from test"
        database = "test"
        schema {
            fields {
                label = STRING
                value = INT
                rt = STRING
                time = BIGINT
            }
        }
    }

}
```

## 变更日志

<ChangeLog />

