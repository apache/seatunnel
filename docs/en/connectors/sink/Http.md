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
| params                      | Map     | No       | -       | HTTP request parameters. For `POST`/`PUT`/`DELETE` requests without a JSON body they are sent as form fields; for `GET` requests they are appended as URL query parameters. |
| retry                       | Int     | No       | -       | The maximum retry count if the HTTP request returns an `IOException`.                                                                                      |
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

`params` is the most flexible way to attach HTTP request parameters:

- For `POST`/`PUT`/`DELETE` requests without a JSON body, the entries are sent as `application/x-www-form-urlencoded` form fields.
- For `GET` requests, the entries are appended to the URL as query string parameters.

If the upstream row itself is a JSON object and the request method is `POST`, the JSON body is sent
as the request payload and `params` is sent alongside it as additional form fields.

### retry behavior

Retries are triggered only for `IOException` responses. The first retry happens after
`retry_backoff_multiplier_ms` (default 100 ms), then each subsequent retry waits
`min(previous_wait * retry_backoff_multiplier_ms, retry_backoff_max_ms)` until either the
retry succeeds or `retry` attempts are exhausted.

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

### With Retry And Form Params

```hocon
sink {
  Http {
    url = "http://localhost/test/webhook"
    headers {
      token = "9e32e859ef044462a257e1fc76730066"
    }
    params {
      source = "seatunnel"
      channel = "cdc"
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