import ChangeLog from '../changelog/connector-graphql.md';

# GraphQL

> GraphQL sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)
- [x] [support multiple table write](../../concept/connector-v2-features.md)

## Description

Used to launch web hooks using data.

> For example, if the data from upstream is [`label: {"__name__": "test1"}, value: 1.2.3,time:2024-08-15T17:00:00`], the body content is the following: `{"label":{"__name__": "test1"}, "value":"1.23","time":"2024-08-15T17:00:00"}`

**Tips: GraphQL sink only support `post json` webhook and the data from source will be treated as body content in web hook.And does not support passing past data**

## Supported DataSource Info

In order to use the Http connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions |                                                    Dependency                                                    |
|------------|--------------------|------------------------------------------------------------------------------------------------------------------|
| Http       | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/seatunnel-connectors-v2/connector-prometheus) |

## Sink Options

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

## Example

simple:

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

## Changelog

<ChangeLog />