import ChangeLog from '../changelog/connector-http-feishu.md';

# Feishu

> Feishu sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Description

Used to launch Feishu webhooks using upstream data.

> For example, if the data from upstream is [`age: 12, name: tyrantlucifer`], the body content is the following: `{"age": 12, "name": "tyrantlucifer"}`

The Feishu sink sends `POST` requests. Each upstream row is converted to JSON and used as the request body. When `array_mode = true`, multiple rows are accumulated into one JSON array before sending.

:::tip

Feishu webhook URLs and custom authentication headers are sensitive. Do not print real tokens in logs or examples.

:::

## Data Type Mapping

|     SeaTunnel Data Type     | Feishu Data Type |
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

| Name                        | Type    | Required | Default | Description                                                                                                 |
|-----------------------------|---------|----------|---------|-------------------------------------------------------------------------------------------------------------|
| url                         | String  | Yes      | -       | Feishu webhook URL. It can include `${database_name}`, `${schema_name}`, and `${table_name}` placeholders in multi-table jobs. |
| headers                     | Map     | No       | -       | HTTP request headers. Use it when the webhook gateway requires extra headers.                               |
| params                      | Map     | No       | -       | Accepted by the option rule. For the current sink writer, put query parameters directly in `url`; rows are posted to the final URL as the request body. |
| retry                       | Int     | No       | -       | The maximum retry times when the HTTP request fails with an `IOException`.                                  |
| retry_backoff_multiplier_ms | Int     | No       | 100     | Retry backoff multiplier in milliseconds.                                                                   |
| retry_backoff_max_ms        | Int     | No       | 10000   | Maximum retry backoff in milliseconds.                                                                      |
| array_mode                  | Boolean | No       | false   | Send rows as a JSON array when true, or as one JSON object per request when false.                          |
| batch_size                  | Int     | No       | 1       | The maximum number of rows sent in one request. Only works when `array_mode` is true.                       |
| request_interval_ms         | Int     | No       | 0       | Interval in milliseconds between two HTTP requests, used to avoid sending requests too frequently.          |
| multi_table_sink_replica    | Int     | No       | 1       | Number of sink replicas used for multi-table write. See [Sink Common Options](../common-options/sink-common-options.md). |
| common-options              |         | No       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details. |

## Usage Notes

- The sink always sends `POST` JSON requests; it does not expose a `method` option.
- If the webhook URL needs query parameters, include them directly in `url`.
- `array_mode` is useful when the receiver accepts a JSON array and you want fewer HTTP requests.
- Feishu webhook delivery is not exactly-once. If a retry happens after the remote service has already handled a request, the receiver may see duplicate messages.

## Task Example

### Simple

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row_num = 1
    schema = {
      fields {
        name = string
        age = int
      }
    }
    rows = [
      {
        fields = [tyrantlucifer, 12]
        kind = INSERT
      }
    ]
  }
}

sink {
  Feishu {
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/xxxx"
  }
}
```

### With headers and retry

```hocon
Feishu {
  url = "https://open.feishu.cn/open-apis/bot/v2/hook/xxxx"
  headers {
    Content-Type = "application/json"
  }
  retry = 3
  retry_backoff_multiplier_ms = 200
  retry_backoff_max_ms = 5000
}
```

### Batch rows as JSON array

```hocon
Feishu {
  url = "https://open.feishu.cn/open-apis/bot/v2/hook/xxxx"
  array_mode = true
  batch_size = 20
  request_interval_ms = 500
}
```

### Multiple table webhook path

```hocon
Feishu {
  url = "https://example.com/feishu/${database_name}/${table_name}"
  multi_table_sink_replica = 2
}
```

## Changelog

<ChangeLog />
