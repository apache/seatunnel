import ChangeLog from '../changelog/connector-graphql.md';

# GraphQL

> GraphQL sink 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [[精确一次]](../../concept/connector-v2-features.md)
- [ ] [变更数据捕获](../../concept/connector-v2-features.md)
- [x] [支持多表写入](../../concept/connector-v2-features.md)

## 描述

接收Source端传入的数据，利用数据触发 web hooks。

> 例如，来自上游的数据为 [`label: {"__name__": "test1"}, value: 1.2.3,time:2024-08-15T17:00:00`], 则body内容如下: `{"label":{"__name__": "test1"}, "value":"1.23","time":"2024-08-15T17:00:00"}`

**Tips: GraphQL 数据接收器 仅支持 `post json` 类型的 web hook，source 数据将被视为 webhook 中的 body 内容。并且不支持传递过去太久的数据**

## 支持的数据源信息

想使用 Prometheus 连接器，需要安装以下必要的依赖。可以通过运行 install-plugin.sh 脚本或者从 Maven 中央仓库下载这些依赖

| 数据源 | 支持版本  | 依赖                                                         |
| ------ | --------- | ------------------------------------------------------------ |
| Http   | universal | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/seatunnel-connectors-v2/connector-prometheus) |

## 接收器选项

|            Name             |  Type  | Required | Default | Description                                                                                                 |
|-----------------------------|--------|----------|---------|-------------------------------------------------------------------------------------------------------------|
| url                         | String | Yes      | -       | Http request url                                                                                            |
| query | String | Yes | - | GraphQL query |
| variables | String | No | - | GraphQL variables |
| valueCover | Boolean | No | - | Whether the data overwrites the variable value |
| headers                     | Map    | No       | -       | Http headers                                                                                                |
| retry                       | Int    | No       | -       | The max retry times if request http return to `IOException`                                                 |
| retry_backoff_multiplier_ms | Int    | No       | 100     | The retry-backoff times(millis) multiplier if request http failed                                           |
| retry_backoff_max_ms        | Int    | No       | 10000   | The maximum retry-backoff times(millis) if request http failed                                              |
| connect_timeout_ms          | Int    | No       | 12000   | Connection timeout setting, default 12s.                                                                    |
| socket_timeout_ms           | Int    | No       | 60000   | Socket timeout setting, default 60s.                                                                        |
| key_timestamp               | Int    | NO       | -       | prometheus timestamp  key .                                                                                 |
| key_label                   | String | yes      | -       | prometheus label key                                                                                        |
| key_value                   | Double | yes      | -       | prometheus value                                                                                            |
| batch_size                  | Int    | false    | 1024       | prometheus batch size write                                                                                 |
| flush_interval              | Long   | false      | 300000L  | prometheus flush commit interval                                                     |
| common-options              |        | No       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details |

## 示例

简单示例:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    tables_configs = [
       {
        schema = {
          table = "graphql_sink_1"
         fields {
                id = int
                val_bool = boolean
                val_int8 = tinyint
                val_int16 = smallint
                val_int32 = int
                val_int64 = bigint
                val_float = float
                val_double = double
                val_decimal = "decimal(16, 1)"
                val_string = string
                val_unixtime_micros = timestamp
      }
        }
            rows = [
              {
                kind = INSERT
                fields = [1, true, 1, 2, 3, 4, 4.3,5.3,6.3, "NEW", "2020-02-02T02:02:02"]
              }
              ]
       },
       {
       schema = {
         table = "graphql_sink_2"
              fields {
                        id = int
                        val_bool = boolean
                        val_int8 = tinyint
                        val_int16 = smallint
                        val_int32 = int
                        val_int64 = bigint
                        val_float = float
                        val_double = double
                        val_decimal = "decimal(16, 1)"
                        val_string = string
                        val_unixtime_micros = timestamp
              }
       }
           rows = [
             {
               kind = INSERT
               fields = [2, true, 1, 2, 3, 4, 4.3,5.3,6.3, "NEW", "2020-02-02T02:02:02"]
             }
             ]
      }
    ]
  }
}

sink {
   GraphQL {
        url = "http://192.168.1.103:9081/v1/graphql"
        query = """
         mutation MyMutation(
           $id: Int!
           $val_bool: Boolean!
           $val_int8: smallint!
           $val_int16: smallint!
           $val_int32: Int!
           $val_int64: bigint!
           $val_float: Float!
           $val_double: Float!
           $val_decimal: numeric!
           $val_string: String!
           $val_unixtime_micros: timestamp!
         ) {
           insert_sink(objects: {
             id: $id,
             val_bool: $val_bool,
             val_int8: $val_int8,
             val_int16: $val_int16,
             val_int32: $val_int32,
             val_int64: $val_int64,
             val_float: $val_float,
             val_double: $val_double,
             val_decimal: $val_decimal,
             val_string: $val_string,
             val_unixtime_micros: $val_unixtime_micros
           }) {
             affected_rows
             returning {
               id
               val_bool
               val_decimal
               val_double
               val_float
               val_int16
               val_int32
               val_int64
               val_int8
               val_string
               val_unixtime_micros
             }
           }
         }
        """
        variables = {
            "val_bool": True
        }
    }
}

```

## 变更日志

<ChangeLog />