import ChangeLog from '../changelog/connector-http-persistiq.md';

# Persistiq

> Persistiq source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Used to read data from Persistiq.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [schema projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

|            name             |  type   | required | default value | description |
|-----------------------------|---------|----------|---------------|-------------|
| url                         | String  | Yes      | -             | Persistiq API endpoint URL. |
| password                    | String  | Yes      | -             | Persistiq API key. Create one from your Persistiq account page. |
| method                      | String  | No       | GET           | HTTP request method. Supported values: `GET`, `POST`. |
| schema                      | Config  | No       | -             | Output schema. Required when `format = "json"`. See [Schema Feature](../../introduction/concepts/schema-feature.md). |
| schema.fields               | Config  | No       | -             | Field definitions for `schema`. |
| format                      | String  | No       | TEXT          | Response format. Supported values: `json`, `text`. Default is `TEXT` (whole body as one `content` column). |
| params                      | Map     | No       | -             | HTTP query parameters appended to the request URL. |
| body                        | String  | No       | -             | HTTP request body. Used together with `POST` when the API expects a JSON payload. |
| json_field                  | Config  | No       | -             | JSONPath mapping from response fields to output columns. Must be used together with `schema`. |
| content_field               | String  | No       | -             | JSONPath that selects the array or object that should be parsed as rows. |
| poll_interval_millis        | int     | No       | -             | Poll interval in milliseconds, used in `STREAMING` mode. Persistiq source is primarily batch-oriented, but you can run the job in `STREAMING` mode to repeatedly poll the API. |
| retry                       | int     | No       | -             | Maximum retry count when the request throws `IOException`. |
| retry_backoff_multiplier_ms | int     | No       | 100           | Retry backoff multiplier in milliseconds. |
| retry_backoff_max_ms        | int     | No       | 10000         | Maximum retry backoff in milliseconds. |
| enable_multi_lines          | boolean | No       | false         | Whether to split the response into lines before parsing each line using the selected `format`. |
| connect_timeout_ms          | int     | No       | 12000         | TCP connect timeout in milliseconds. |
| socket_timeout_ms           | int     | No       | 60000         | Socket read timeout in milliseconds. |
| json_filed_missed_return_null | boolean | No     | false         | Return `null` when a field configured in `json_field` is missing from the response. |
| common-options              | config  | No       | -             | Source common options. See [Source Common Options](../common-options/source-common-options.md). |

### url [String]

The Persistiq API endpoint. For example `https://api.persistiq.com/v1/users`.

### password [String]

API key for login, you can get it at Persistiq website. The connector sends this API key in the `x-api-key` HTTP header.

### method [String]

http request method, only supports GET, POST method

### params [Map]

http params

### body [String]

http body

### poll_interval_millis [int]

request http api interval(millis) in stream mode

### retry [int]

The max retry times if request http return to `IOException`

### retry_backoff_multiplier_ms [int]

The retry-backoff times(millis) multiplier if request http failed

### retry_backoff_max_ms [int]

The maximum retry-backoff times(millis) if request http failed

### format [String]

the format of upstream data, now only support `json` `text`, default `text`.

when you assign format is `json`, you should also assign schema option, for example:

upstream data is the following:

```json
{
  "code": 200,
  "data": "get success",
  "success": true
}
```

you should assign schema as the following:

```hocon

schema {
    fields {
        code = int
        data = string
        success = boolean
    }
}

```

connector will generate data as the following:

| code |    data     | success |
|------|-------------|---------|
| 200  | get success | true    |

when you assign format is `text`, connector will do nothing for upstream data, for example:

upstream data is the following:

```json
{
  "code": 200,
  "data": "get success",
  "success": true
}
```

connector will generate data as the following:

|                         content                          |
|----------------------------------------------------------|
| {"code":  200, "data":  "get success", "success":  true} |

### schema [Config]

#### fields [Config]

The schema fields of upstream data. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md).

### content_field [String]

This parameter can get some json data. If you only need the data in the 'book' section, configure `content_field = "$.store.book.*"`.

If your return data looks something like this.

```json
{
  "store": {
    "book": [
      {
        "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      {
        "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      }
    ],
    "bicycle": {
      "color": "red",
      "price": 19.95
    }
  },
  "expensive": 10
}
```

You can configure `content_field = "$.store.book.*"` and the result returned looks like this:

```json
[
  {
    "category": "reference",
    "author": "Nigel Rees",
    "title": "Sayings of the Century",
    "price": 8.95
  },
  {
    "category": "fiction",
    "author": "Evelyn Waugh",
    "title": "Sword of Honour",
    "price": 12.99
  }
]
```

The illustrative mock endpoint below must return the `store.book` fixture above.

Then you can get the desired result with a simpler schema, like:

```hocon
source {
  Persistiq {
    url = "http://mock-http:8080/books"
    password = "your-api-key"
    format = "json"
    content_field = "$.store.book.*"
    schema = {
      fields {
        category = string
        author = string
        title = string
        price = double
      }
    }
  }
}
```

### json_field [Config]

This parameter helps you configure the schema, so this parameter must be used with schema.

If your data looks something like this:

```json
{
  "store": {
    "book": [
      {
        "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      {
        "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      }
    ],
    "bicycle": {
      "color": "red",
      "price": 19.95
    }
  },
  "expensive": 10
}
```

This illustrative mock endpoint must return the `store.book` JSON fixture above; it is not a Persistiq users response.

You can get the contents of 'book' by configuring the task as follows:

```hocon
source {
  Persistiq {
    url = "http://mock-http:8080/books"
    password = "your-api-key"
    format = "json"
    json_field = {
      category = "$.store.book[*].category"
      author = "$.store.book[*].author"
      title = "$.store.book[*].title"
      price = "$.store.book[*].price"
    }
    schema = {
      fields {
        category = string
        author = string
        title = string
        price = string
      }
    }
  }
}
```

### Pagination limitation

The current Persistiq reader does not pass pagination state to `HttpSourceReader`. Each request
parses one response; `pageing` does not advance a page number or cursor in this connector. Streaming
polling can repeat that response and does not provide a complete paginated export. Filter the API
request to the desired response, and do not assume rows from later pages are read. Multi-page support
requires a separately implemented and tested reader path.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details

## Example

### Read users from Persistiq

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Persistiq {
    url = "https://api.persistiq.com/v1/users"
    password = "your-api-key"
    format = "json"
    content_field = "$.users.*"
    schema = {
      fields {
        id = string
        name = string
        email = string
        activated = boolean
        default_mailbox_id = string
        salesforce_id = string
      }
    }
  }
}

sink {
  Console {}
}
```

### Streaming poll against Persistiq

Persistiq does not provide a streaming endpoint, but the connector can still run in `STREAMING` mode
and poll the API on `poll_interval_millis`. Each poll runs the configured request once and emits the
resulting rows. The reader does not persist API cursors or consumed-row offsets in checkpoints. Polling
can emit the same API objects again, and a restarted job begins its requests again; use an idempotent
downstream sink when repeated records are possible.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Persistiq {
    url = "https://api.persistiq.com/v1/users"
    password = "your-api-key"
    poll_interval_millis = 60000
    format = "json"
    content_field = "$.users.*"
    schema = {
      fields {
        id = string
        name = string
        email = string
      }
    }
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
