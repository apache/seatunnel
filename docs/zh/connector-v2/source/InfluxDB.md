# InfluxDB

> InfluxDB source connector

## Description

通过InfluxDB读取外部数据源数据。

## Support InfluxDB Version

- 1.x/2.x

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Using Dependency

### For Spark/Flink Engine

> 1. 需要确保连接器Jar包 [influxDB connector jar package](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-influxdb) 被放在目录 `${SEATUNNEL_HOME}/connectors/`.

### For SeaTunnel Zeta Engine

> 1. 需要确保连接器Jar包 [influxDB connector jar package](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-influxdb) 被放在目录 `${SEATUNNEL_HOME}/lib/`.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support multiple table reading](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Data Type Mapping

|                                 InfluxDB Data Type                                  | SeaTunnel Data Type |
|-------------------------------------------------------------------------------------|---------------------|
| BOOLEAN                                                                             | BOOLEAN             |
| SMALLINT                                                                            | SHORT               |
| INT                                                                                 | INTEGER             |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR | INT                 |
| BIGINT                                                                              | LONG                |
| FLOAT<br/>DOUBLE                                                                    | DOUBLE              |
| STRING                                                                              | STRING              |

## Options

|        name        |  type  | required | default value |
|--------------------|--------|----------|---------------|
| url                | string | yes      | -             |
| sql                | string | yes      | -             |
| schema             | config | yes      | -             |
| database           | string | yes      |               |
| username           | string | no       | -             |
| password           | string | no       | -             |
| lower_bound        | long   | no       | -             |
| upper_bound        | long   | no       | -             |
| partition_num      | int    | no       | -             |
| split_column       | string | no       | -             |
| epoch              | string | no       | n             |
| connect_timeout_ms | long   | no       | 15000         |
| query_timeout_sec  | int    | no       | 3             |
| chunk_size         | int    | no       | 0             |
| common-options     | config | no       | -             |

### url

连接到InfluxDB的url，例如：

```
http://influxdb-host:8086
```

### sql [string]

用于查询数据的SQL

```
select name,age from test
```

### schema [config]

#### fields [Config]

上游数据的schema信息，例如：

```
schema {
    fields {
        name = string
        age = int
    }
  }
```

### database [string]

influxDB 库

### username [string]

InfluxDB 用户名

### password [string]

InfluxDB 密码

### split_column [string]

InfluxDB 分片列

> Tips:
> - InfluxDB的标签不支持作为分片主键，因为标签类型只能是字符串
> - InfluxDB的时间不支持作为分片主键，因为时间字段不能参与数学计算
> - 目前，split_column仅支持Integer类型分片，并不支持float、string、date等类型。

### upper_bound [long]

分片字段数据的上限

### lower_bound [long]

分片字段数据的下限

```
     将$split_column范围分片成$partition_num部分
     如果partition_num为1，则使用整个`split_column`范围
     如果partition_num < (upper_bound - lower_bound)，则使用(upper_bound - lower_bound)分片
     
     例如: lower_bound = 1, upper_bound = 10, partition_num = 2
     sql = "select * from test where age > 0 and age < 10"
     
     分片结果

     分片1: select * from test where ($split_column >= 1 and $split_column < 6)  and (  age > 0 and age < 10 )
     
     分片2: select * from test where ($split_column >= 6 and $split_column < 11) and (  age > 0 and age < 10 )
```

### partition_num [int]

分片数量

> 提示: 确保upper_bound减lower_bound能被partition_num整除，否则查询结果会重叠

### epoch [string]

返回的时间精度
可选值: H, m, s, MS, u, n
默认值: n

### query_timeout_sec [int]

InfluxDB的查询超时时间，单位为秒

### connect_timeout_ms [long]

连接InfluxDB的超时时间，单位为毫秒

### common options

插件公共参数，请参考 [公共选项](common-options.md)

## Examples

多分片查询的示例

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

```

不使用分片查询的示例

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
```

使用分块查询的示例

```hocon
source {
    InfluxDB {
        url = "http://influxdb-host:8086"
        sql = "select label, value, rt, time from test"
        database = "test"
        chunk_size = 100000
        schema {
            fields {
                label = STRING
                value = INT
                rt = STRING
                time = BIGINT
            }
    }
}
```

> Tips:
> - 分块查询是为了解决没有办法找到合适的分片列进行分片查询，但同时数据量又大的情况。所以如果配置了split_column或者chunk_size = 0则不进行分块查询。
> - 使用分块查询时，source并行度只能为1，但速度仍然很快，将对下游造成压力，建议将下游的并行度调大，或者输出速率调大，减少反压，提高性能。
> - 使用分块查询时，会对influxDB数据库本身造成压力，与数据量成正比。实测，seatunnel同步了20多G数据时，influxdb的内存上升10多G。

## 更新日志

### 2.2.0-beta 2022-09-26

- Add InfluxDB Source Connector

