# Prometheus

> Prometheus sink connector

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

**Tips: Prometheus sink only support `post json` webhook and the data from source will be treated as body content in web hook.And does not support passing past data**

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
    schema = {
      fields {
        c_map = "map<string, string>"
        c_double = double
        c_timestamp = timestamp
      }
    }
    result_table_name = "fake"
    rows = [
       {
         kind = INSERT
         fields = [{"__name__": "test1"},  1.23, "2024-08-15T17:00:00"]
       },
       {
         kind = INSERT
         fields = [{"__name__": "test2"},  1.23, "2024-08-15T17:00:00"]
       }
    ]
  }
}


sink {
  Prometheus {
    url = "http://prometheus:9090/api/v1/write"
    key_label = "c_map"
    key_value = "c_double"
    key_timestamp = "c_timestamp"
    batch_size = 1
  }
}

```

## Changelog

### 2.3.8-beta 2024-08-22

- Add Http Sink Connector

