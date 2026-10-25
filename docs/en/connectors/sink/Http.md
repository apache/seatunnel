import ChangeLog from '../changelog/connector-http.md';

# Http

> Http sink connector

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

Used to launch web hooks using upstream data. The Http sink always sends `POST` requests; each
upstream row is serialized to JSON and used as the request body.

> For example, if the data from upstream is [`age: 12, name: tyrantlucifer`], the body content is the following: `{"age": 12, "name": "tyrantlucifer"}`

**Tips: Http sink only supports `POST json` webhooks and the data from the source will be treated as body content in the webhook.**

## Supported DataSource Info

In order to use the Http connector, the following dependencies are required. They can be
downloaded via `install-plugin.sh` or from the Maven central repository.

| Datasource | Supported Versions | Dependency                                                                         |
|------------|--------------------|------------------------------------------------------------------------------------|
| Http       | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-http) |

## Sink Options

|            Name             |  Type   | Required | Default | Description                                                                                                                                                |
|-----------------------------|---------|----------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                         | String  | Yes      | -       | Http request URL. Static query parameters can be embedded directly in the URL.                                                                            |
| headers                     | Map     | No       | -       | HTTP headers added to every request. Each entry is one `name = "value"` pair.                                                                              |
| params                      | Map     | No       | -       | The current sink writer does not use `params`. Put query parameters directly in `url`; rows are sent as the POST body. |
| retry                       | Int     | No       | -       | When at least 1, the maximum total HTTP attempts, including the first request. Only `IOException` failures are retried. |
| retry_backoff_multiplier_ms | Int     | No       | 100     | The retry-backoff time (millis) multiplier applied between retries.                                                                                         |
| retry_backoff_max_ms        | Int     | No       | 10000   | The maximum retry-backoff time (millis) between retries.                                                                                                   |
| array_mode                  | Boolean | No       | false   | When `true`, rows are accumulated into a JSON array before being sent. When `false`, each row is sent as a single JSON object.                            |
| batch_size                  | Int     | No       | 1       | The maximum number of rows sent in one HTTP request. Only takes effect when `array_mode = true`.                                                          |
| request_interval_ms         | Int     | No       | 0       | The interval in milliseconds between two HTTP requests, used to avoid sending requests too frequently.                                                    |
| multi_table_sink_replica    | Int     | No       | -       | Number of sink writer replicas used when writing multiple tables. See [Sink Common Options](../common-options/sink-common-options.md).                      |
| common-options              |         | No       | -       | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.                              |

### url

The HTTP endpoint that receives the webhook. Static query parameters can be embedded in the URL
directly, for example `http://localhost/test/webhook?source=seatunnel`.

### params

The Http sink always sends `POST` requests with JSON row data as the body. The
current writer does not read `params`; put static query parameters directly in
`url`, for example `http://localhost/test/webhook?source=seatunnel`.

### retry behavior

Only `IOException` failures are retried. When `retry >= 1`, it limits the total
number of attempts, including the initial request: `retry = 3` allows at most
three attempts (two retries). The waits follow a Fibonacci sequence:
`min(Fibonacci(attempt_number) * retry_backoff_multiplier_ms, retry_backoff_max_ms)`.
With the default multiplier of 100 ms, successive retry waits are 100, 100, 200,
300, 500 ms, capped by `retry_backoff_max_ms`.

The sink treats a non-200 HTTP response or a request exception as a write failure and reports it to the engine. A failed batch is not acknowledged as successfully flushed. The connector does not provide exactly-once delivery, so receivers should remain idempotent.

### array_mode and batch_size

When `array_mode = false` (the default), the Http sink sends one HTTP request per row. Set
`array_mode = true` to send rows in batches; `batch_size` then controls how many rows go into one
JSON-array request, and `request_interval_ms` adds a delay between consecutive batches.

## Example

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
        age = "int"
        name = "string"
      }
    }
  }
}

sink {
  Http {
    url = "http://localhost/test/webhook"
    headers {
      token = "9e32e859ef044462a257e1fc76730066"
    }
  }
}
```

### With Batch Processing

```hocon
sink {
  Http {
    url = "http://localhost/test/webhook"
    headers {
      token = "9e32e859ef044462a257e1fc76730066"
      Content-Type = "application/json"
    }
    array_mode = true
    batch_size = 50
    request_interval_ms = 500
  }
}
```

### With Retry And URL Query Parameters

```hocon
sink {
  Http {
    url = "http://localhost/test/webhook?source=seatunnel&channel=cdc"
    headers {
      token = "9e32e859ef044462a257e1fc76730066"
    }
    retry = 3
    retry_backoff_multiplier_ms = 200
    retry_backoff_max_ms = 5000
  }
}
```

### Multiple Tables

Use `${database_name}` and `${table_name}` placeholders in the URL to route rows from different
upstream tables to different endpoints.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"

    table-names = ["seatunnel.role", "seatunnel.user", "galileo.Bucket"]
  }
}

transform {
}

sink {
  Http {
    url = "http://localhost/test/${database_name}_test/${table_name}_test"
    headers {
      token = "9e32e859ef044462a257e1fc76730066"
    }
  }
}
```

When the upstream source uses a schema-qualified table list (for example Oracle), use
`${schema_name}` instead of `${database_name}`:

```hocon
source {
  Jdbc {
    driver = oracle.jdbc.driver.OracleDriver
    url = "jdbc:oracle:thin:@localhost:1521/XE"
    username = "testUser"
    password = "testPassword"

    table_list = [
      {
        table_path = "TESTSCHEMA.TABLE_1"
      },
      {
        table_path = "TESTSCHEMA.TABLE_2"
      }
    ]
  }
}

transform {
}

sink {
  Http {
    url = "http://localhost/test/${schema_name}_test/${table_name}_test"
    headers {
      token = "9e32e859ef044462a257e1fc76730066"
    }
  }
}
```

## Changelog

<ChangeLog />
