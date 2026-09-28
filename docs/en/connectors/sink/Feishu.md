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
| DATE                        | String           |
| TIME                        | String           |
| TIMESTAMP                   | String           |
| ARRAY                       | JsonArray        |

## Sink Options

| Name                        | Type    | Required | Default | Description                                                                                                             |
|-----------------------------|---------|----------|---------|-------------------------------------------------------------------------------------------------------------------------|
| url                         | String  | Yes      | -       | Feishu webhook URL. The current sink writer sends requests to this fixed URL and does not replace table-name placeholders. |
| headers                     | Map     | No       | -       | HTTP request headers. Use it when the webhook gateway requires extra headers.                                           |
| params                      | Map     | No       | -       | Accepted by the option rule, but the current sink writer does not pass it to requests. Put non-sensitive query parameters directly in `url` when needed. |
| retry                       | Int     | No       | -       | The maximum retry times when the HTTP request fails with an `IOException`.                                              |
| retry_backoff_multiplier_ms | Int     | No       | 100     | Retry backoff multiplier in milliseconds.                                                                               |
| retry_backoff_max_ms        | Int     | No       | 10000   | Maximum retry backoff in milliseconds.                                                                                  |
| array_mode                  | Boolean | No       | false   | Send rows as a JSON array when true, or as one JSON object per request when false.                                      |
| batch_size                  | Int     | No       | 1       | The maximum number of rows sent in one request. Only works when `array_mode` is true.                                   |
| request_interval_ms         | Int     | No       | 0       | Interval in milliseconds between two HTTP requests, used to avoid sending requests too frequently.                      |
| multi_table_sink_replica    | Int     | No       | 1       | Number of sink replicas used for multi-table write. See [Sink Common Options](../common-options/sink-common-options.md). |
| common-options              |         | No       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details. |

## Usage Notes

- The sink always sends `POST` JSON requests; it does not expose a `method` option.
- If the webhook URL needs query parameters, include non-sensitive parameters directly in `url`. Prefer `headers` for authentication material when the gateway supports it, because the full URL, including its query string, may appear in logs or job metadata.
- Multi-table jobs can use `multi_table_sink_replica`, but the Feishu sink sends every row to the configured fixed `url`. It does not replace `${database_name}`, `${schema_name}`, or `${table_name}` in the URL.
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
    row.num = 1
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
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/<your-hook-token>"
  }
}
```

### With headers and retry

```hocon
Feishu {
  url = "https://open.feishu.cn/open-apis/bot/v2/hook/<your-hook-token>"
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
  url = "https://open.feishu.cn/open-apis/bot/v2/hook/<your-hook-token>"
  array_mode = true
  batch_size = 20
  request_interval_ms = 500
}
```

### Multi-table sink replica

```hocon
Feishu {
  url = "https://open.feishu.cn/open-apis/bot/v2/hook/<your-hook-token>"
  multi_table_sink_replica = 2
}
```

### Stream alerts to a Feishu bot

For continuous alerting, run the sink in streaming mode and use `request_interval_ms`
plus `array_mode = true` to batch multiple alerts into one webhook call. The
example reads events from a Kafka topic and posts them as a single JSON array per
batch.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  Kafka {
    plugin_output = "alerts"
    bootstrap.servers = "kafka:9092"
    topic = "service_alerts"
    format = "json"
    schema = {
      fields {
        name = string
        age = int
      }
    }
  }
}

sink {
  Feishu {
    plugin_input = "alerts"
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/<your-hook-token>"
    array_mode = true
    batch_size = 20
    request_interval_ms = 1000
    retry = 5
    retry_backoff_multiplier_ms = 200
    retry_backoff_max_ms = 10000
  }
}
```

### Send a Feishu Rich Text Message

Feishu bot webhooks expect a `msg_type` envelope such as `text`, `post`, or
`interactive`. Use a `Transform` upstream to wrap each row before the Feishu
sink sends it. The example below sends each row as a `text` envelope.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "raw"
    row.num = 1
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

transform {
  Sql {
    plugin_input = "raw"
    query = "SELECT 'text' AS msg_type, MAP('text', concat('User ', name, ' is ', cast(age as string), ' years old')) AS content FROM raw"
  }
}

sink {
  Feishu {
    plugin_input = "Sql"
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/<your-hook-token>"
    headers {
      Content-Type = "application/json"
    }
  }
}
```

## Changelog

<ChangeLog />
