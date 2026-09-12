import ChangeLog from '../changelog/connector-http-klaviyo.md';

# Klaviyo

> Klaviyo source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Reads data from Klaviyo APIs. The connector builds Klaviyo request headers from `private_key` and `revision`, then uses the shared HTTP source runtime to parse the response.

The Klaviyo connector shares its HTTP request, retry, and pagination runtime with other HTTP-based source connectors. Configure `private_key` with the Klaviyo private API key, set `revision` to the API revision date (for example `2020-10-17`), and point `url` at the Klaviyo endpoint you want to call.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

:::tip

In streaming mode, the connector repeatedly calls the configured API. Set `poll_interval_millis` to control the request interval.

:::

## Supported DataSource Info

In order to use the Klaviyo connector, the following dependency is required.
It can be downloaded via install-plugin.sh or from the Maven central repository.

| Datasource   | Supported Versions |                                         Dependency                                          |
|--------------|--------------------|-----------------------------------------------------------------------------------------------|
| Klaviyo      | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-http-base)       |

## Options

| name                        | type   | required | default value | description |
|-----------------------------|--------|----------|---------------|-------------|
| url                         | String | Yes      | -             | Klaviyo API URL. |
| private_key                 | String | Yes      | -             | Klaviyo private API key. |
| revision                    | String | Yes      | -             | Klaviyo API revision, usually in `YYYY-MM-DD` format. |
| method                      | String | No       | GET           | HTTP method. Supported values are `GET` and `POST`. |
| headers                     | Map    | No       | -             | Extra HTTP request headers. The connector already sets `Authorization`, `Accept`, and `revision`. |
| params                      | Map    | No       | -             | HTTP query parameters. |
| body                        | String | No       | -             | HTTP request body, usually used with `POST`. |
| format                      | String | No       | TEXT          | Response format. Use `json` when the response should be parsed by `schema`, `json_field`, or `content_field`. |
| schema                      | Config | No       | -             | Output schema. Required when `format = "json"`. |
| json_field                  | Config | No       | -             | JSONPath mapping from response fields to output columns. Must be used together with `schema`. |
| content_field               | String | No       | -             | JSONPath used to select the array or object that should be parsed as rows. |
| pageing                     | Config | No       | -             | Pagination settings. See [Pagination](#pagination). |
| poll_interval_millis        | int    | No       | -             | Request interval in milliseconds for streaming jobs. |
| retry                       | int    | No       | -             | Maximum retry count when the request throws `IOException`. |
| retry_backoff_multiplier_ms | int    | No       | 100           | Retry backoff multiplier in milliseconds. |
| retry_backoff_max_ms        | int    | No       | 10000         | Maximum retry backoff in milliseconds. |
| json_filed_missed_return_null | boolean | No    | false         | Return `null` when a field configured in `json_field` is missing. |
| common-options              | config | No       | -             | Source common options. See [Source Common Options](../common-options/source-common-options.md). |

### Authentication

Set `private_key` to the Klaviyo private API key. The connector sends it as:

```text
Authorization: Klaviyo-API-Key <private_key>
Accept: application/json
revision: <revision>
```

### Response Parsing

The default `format` is `TEXT`, which returns the whole response as one `content` column.

Use `format = "json"` and `schema` when you want structured output:

```hocon
format = "json"
schema = {
  fields {
    type = string
    id = string
    attributes = {
      name = string
      created = string
      updated = string
    }
    links = {
      self = string
    }
  }
}
```

Use `content_field` when the rows are inside a nested JSON node. Use `json_field` when columns need to be extracted from different JSONPath expressions.

### Pagination

`pageing` can be used when the target API needs paging parameters.

| name | type | required | default value | description |
|------|------|----------|---------------|-------------|
| total_page_size | long | No | 0 | Total number of pages to request. |
| batch_size | int | No | 100 | Page size returned by each request. |
| start_page_number | long | No | 1 | First page number. |
| page_field | String | No | page | Request parameter name for page number pagination. |
| page_type | String | No | PageNumber | Pagination type. Supported values are `PageNumber` and `Cursor`. |
| cursor_field | String | No | - | Request parameter name for cursor pagination. |
| cursor_response_field | String | No | - | JSONPath field used to read the next cursor from the response. |
| use_placeholder_replacement | boolean | No | false | Use `${field}` placeholder replacement in headers, parameters, and body. |

## Example

### Batch Read of a Klaviyo List

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Klaviyo {
    plugin_output = "klaviyo"
    url = "https://a.klaviyo.com/api/lists"
    private_key = "replace-with-private-key"
    revision = "2020-10-17"
    method = "GET"
    format = "json"
    schema = {
      fields {
        type = string
        id = string
        attributes = {
          name = string
          created = string
          updated = string
        }
        links = {
          self = string
        }
      }
    }
  }
}

sink {
  Console {
    plugin_input = "klaviyo"
  }
}
```

### Read with Cursor-Based Pagination

Use `pageing` to follow the `next` cursor that Klaviyo returns. The connector
reads `cursor_response_field` from the response, writes the value back to
`cursor_field`, and continues until the response no longer carries a cursor.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Klaviyo {
    plugin_output = "klaviyo_cursor"
    url = "https://a.klaviyo.com/api/events"
    private_key = "replace-with-private-key"
    revision = "2020-10-17"
    method = "GET"
    format = "json"
    pageing = {
      page_type = "Cursor"
      cursor_field = "page[cursor]"
      cursor_response_field = "$.links.next"
    }
    schema = {
      fields {
        type = string
        id = string
        attributes = {
          name = string
          created = string
          updated = string
        }
      }
    }
  }
}

sink {
  Console {
    plugin_input = "klaviyo_cursor"
  }
}
```

### Polling Read in Streaming Mode

For endpoints that grow over time, run the connector in `STREAMING` mode and
let `poll_interval_millis` decide how often SeaTunnel re-issues the same
request.

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Klaviyo {
    plugin_output = "klaviyo_stream"
    url = "https://a.klaviyo.com/api/metrics"
    private_key = "replace-with-private-key"
    revision = "2020-10-17"
    method = "GET"
    poll_interval_millis = 30000
    format = "json"
    schema = {
      fields {
        type = string
        id = string
        attributes = {
          name = string
          created = string
          updated = string
        }
      }
    }
  }
}

sink {
  Console {
    plugin_input = "klaviyo_stream"
  }
}
```

## Changelog

<ChangeLog />
