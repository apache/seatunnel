# Feishu

> Feishu sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

## Description

Used to launch Feishu web hooks using data.

> For example, if the data from upstream is [`age: 12, name: tyrantlucifer`], the body content is the following: `{"age": 12, "name": "tyrantlucifer"}`

**Tips: Feishu sink only support `post json` webhook and the data from source will be treated as body content in web hook.**

## Data Type Mapping

|     Seatunnel Data type     | Feishu Data type |
|-----------------------------|------------------|
| ROW<br/>MAP                 | Json             |
| NULL                        | null             |
| BOOLEAN                     | boolean          |
| TINYINT                     | byte             |
| SMALLINT                    | short            |
| INT                         | int              |
| BIGINT                      | long             |
| FLOAT                       | float            |
| DOUBLE                      | double           |
| DECIMAL                     | BigDecimal       |
| BYTES                       | byte[]           |
| STRING                      | String           |
| TIME<br/>TIMESTAMP<br/>TIME | String           |
| ARRAY                       | JsonArray        |

## Sink Options

|      Name      |  Type  | Required | Default |                                             Description                                             |
|----------------|--------|----------|---------|-----------------------------------------------------------------------------------------------------|
| url            | String | Yes      | -       | Feishu webhook url                                                                                  |
| headers        | Map    | No       | -       | Http request headers                                                                                |
| common-options |        | no       | -       | Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details |

## Task Example

### Simple:

```hocon
Feishu {
        url = "https://www.feishu.cn/flow/api/trigger-webhook/108bb8f208d9b2378c8c7aedad715c19"
    }
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Feishu Sink Connector

