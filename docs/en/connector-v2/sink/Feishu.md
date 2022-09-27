# Feishu

> Feishu sink connector

## Description

Used to launch feishu web hooks using data. 

> For example, if the data from upstream is [`age: 12, name: tyrantlucifer`], the body content is the following: `{"age": 12, "name": "tyrantlucifer"}`

**Tips: Feishu sink only support `post json` webhook and the data from source will be treated as body content in web hook.**

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

##  Options

| name | type   | required | default value |
| --- |--------| --- | --- |
| url | String | Yes | - |
| headers | Map    | No | - |
| common-options| string| no | -  |

### url [string]

Feishu webhook url

### headers [Map]

Http request headers

### common options [string]

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## Example

simple:

```hocon
Feishu {
        url = "https://www.feishu.cn/flow/api/trigger-webhook/108bb8f208d9b2378c8c7aedad715c19"
    }
```

