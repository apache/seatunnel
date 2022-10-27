# Http

> Http sink connector

## Description

Used to launch web hooks using data.

> For example, if the data from upstream is [`age: 12, name: tyrantlucifer`], the body content is the following: `{"age": 12, "name": "tyrantlucifer"}`

**Tips: Http sink only support `post json` webhook and the data from source will be treated as body content in web hook.**

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

##  Options

| name                               | type   | required | default value |
|------------------------------------|--------|----------|---------------|
| url                                | String | Yes      | -             |
| headers                            | Map    | No       | -             |
| params                             | Map    | No       | -             |
| retry                              | int    | No       | -             |
| retry_backoff_multiplier_ms        | int    | No       | 100           |
| retry_backoff_max_ms               | int    | No       | 10000         |
| common-options                     |        | no       | -             |

### url [String]

http request url

### headers [Map]

http headers

### params [Map]

http params

### retry [int]

The max retry times if request http return to `IOException`

### retry_backoff_multiplier_ms [int]

The retry-backoff times(millis) multiplier if request http failed

### retry_backoff_max_ms [int]

The maximum retry-backoff times(millis) if request http failed

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

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
