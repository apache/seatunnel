# Http

> Http sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

## Description

Used to launch web hooks using data.

> For example, if the data from upstream is [`age: 12, name: tyrantlucifer`], the body content is the following: `{"age": 12, "name": "tyrantlucifer"}`

**Tips: Http sink only support `post json` webhook and the data from source will be treated as body content in web hook.**

## Supported DataSource Info

In order to use the Http connector, the following dependencies are required.
They can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource | Supported Versions |                                                 Dependency                                                 |
|------------|--------------------|------------------------------------------------------------------------------------------------------------|
| Http       | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/seatunnel-connectors-v2/connector-http) |

## Sink Options

|            Name             |  Type  | Required | Default |                                             Description                                             |
|-----------------------------|--------|----------|---------|-----------------------------------------------------------------------------------------------------|
| url                         | String | Yes      | -       | Http request url                                                                                    |
| headers                     | Map    | No       | -       | Http headers                                                                                        |
| params                      | Map    | No       | -       | Http params                                                                                         |
| retry                       | Int    | No       | -       | The max retry times if request http return to `IOException`                                         |
| retry_backoff_multiplier_ms | Int    | No       | 100     | The retry-backoff times(millis) multiplier if request http failed                                   |
| retry_backoff_max_ms        | Int    | No       | 10000   | The maximum retry-backoff times(millis) if request http failed                                      |
| common-options              |        | No       | -       | Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details |

## Example

simple:

```hocon
Http {
    url = "http://localhost/test/webhook"
    headers {
        token = "9e32e859ef044462a257e1fc76730066"
    }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Http Sink Connector

