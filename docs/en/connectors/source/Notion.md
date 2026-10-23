import ChangeLog from '../changelog/connector-http-notion.md';

# Notion

> Notion source connector

## Description

The Notion source connector reads data from the Notion API. It is based on the HTTP source connector and automatically adds the `Authorization: Bearer <password>` and `Notion-Version: <version>` headers from the connector options.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Source Options

| Name                        | Type    | Required | Default | Description |
|-----------------------------|---------|----------|---------|-------------|
| url                         | String  | Yes      | -       | Notion API request URL, for example `https://api.notion.com/v1/users`. |
| password                    | String  | Yes      | -       | Notion integration token. The connector sends it as the `Authorization` bearer token. |
| version                     | String  | Yes      | -       | Notion API version, for example `2022-06-28`. The connector sends it as the `Notion-Version` header. |
| method                      | String  | No       | get     | HTTP request method. Supported values are `GET` and `POST`. |
| headers                     | Map     | No       | -       | Extra HTTP headers. `Authorization` and `Notion-Version` are set by `password` and `version`. |
| params                      | Map     | No       | -       | Query parameters sent with the request. |
| body                        | String  | No       | -       | HTTP request body. Usually used with `method = "POST"`. |
| format                      | String  | No       | text    | Response format. Use `json` when reading Notion JSON into a SeaTunnel schema; use `text` to return the raw response as `content`. |
| schema                      | Config  | No       | -       | Output schema. Required when `format = "json"`. |
| schema.fields               | Config  | No       | -       | Field names and SeaTunnel data types used to parse the JSON response. |
| content_field               | String  | No       | -       | JSONPath used to select a nested part of the response before parsing it with `schema`. |
| json_field                  | Config  | No       | -       | Field-level JSONPath mapping. Use it with `schema` when output fields come from different JSON paths. |
| pageing                     | Config  | No       | -       | HTTP pagination settings inherited from the HTTP source connector. |
| page_type                   | String  | No       | PageNumber | Pagination type. Supported values are `PageNumber` (default) and `Cursor`. Use `Cursor` for Notion endpoints that return a `next_cursor`. |
| cursor_field                | String  | No       | -       | The request parameter name that carries the cursor value. Used together with `page_type = "Cursor"`. |
| cursor_response_field       | String  | No       | -       | The JSONPath of the cursor in the response body. Used together with `page_type = "Cursor"`. |
| poll_interval_millis        | Int     | No       | -       | Request interval in milliseconds when the source is used in streaming mode. Notion source currently supports batch mode only. |
| retry                       | Int     | No       | -       | Maximum retry times when the HTTP request fails with an `IOException`. |
| retry_backoff_multiplier_ms | Int     | No       | 100     | Retry backoff multiplier in milliseconds. |
| retry_backoff_max_ms        | Int     | No       | 10000   | Maximum retry backoff in milliseconds. |
| enable_multi_lines          | Boolean | No       | false   | When `true`, multiple JSON objects separated by newlines in the response body are treated as separate records. |
| keep_params_as_form         | Boolean | No       | false   | When `true`, request parameters are sent as form-encoded body parameters instead of URL query parameters. |
| keep_page_param_as_http_param | Boolean | No    | false   | When `true`, the page parameter remains in the request URL when paginating instead of being replaced inside the body. |
| batch_size                  | Int     | No       | 100     | The number of records returned per page request when the total number of pages is unknown. |
| start_page_number           | Long    | No       | 1       | Which page number to start synchronizing from. |
| total_page_size             | Long    | No       | 0       | Total page size to read. `0` means use `batch_size` until the API stops returning new pages. |
| use_placeholder_replacement | Boolean | No       | false   | When `true`, use `${field}` placeholder replacement for headers, parameters and body values; otherwise use key-based replacement. |
| connect_timeout_ms          | Int     | No       | 12000   | HTTP connection timeout in milliseconds. Default 12000ms. |
| socket_timeout_ms           | Int     | No       | 60000   | HTTP socket timeout in milliseconds. Default 60000ms. |
| json_filed_missed_return_null | Boolean | No    | false   | Return null when a configured JSON field is missing. |
| common-options              | Config  | No       | -       | Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md). |

:::tip

`password` is a sensitive Notion integration token. Avoid hardcoding real tokens in shared job files. Use SeaTunnel variable substitution or your deployment secret mechanism when possible.

:::

## Usage Notes

- Set `format = "json"` and configure `schema` when you want typed SeaTunnel rows.
- Use `content_field` when the Notion response wraps records in a nested array, such as `$.results.*`.
- Use `json_field` only when each output field needs its own JSONPath expression.
- `password` and `version` override the `Authorization` and `Notion-Version` headers. Put only other custom headers in `headers`.
- For Notion list endpoints, prefer `page_type = "Cursor"` together with `cursor_field = "start_cursor"` and `cursor_response_field = "$.next_cursor"`.
- The Notion source supports batch mode only. `poll_interval_millis` is exposed for HTTP-base compatibility but does not enable streaming behavior.

## Task Examples

### Read Users

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Notion {
    url = "https://api.notion.com/v1/users"
    password = "<notion-integration-token>"
    version = "2022-06-28"
    method = "GET"
    format = "json"
    content_field = "$.results.*"
    schema = {
      fields {
        object = string
        id = string
        type = string
        person = {
          email = string
        }
        name = string
        avatar_url = string
      }
    }
  }
}

sink {
  Console {
  }
}
```

### Search Pages With Cursor Pagination

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Notion {
    url = "https://api.notion.com/v1/search"
    password = "<notion-integration-token>"
    version = "2022-06-28"
    method = "POST"
    body = "{\"page_size\": 100, \"filter\": {\"value\": \"page\", \"property\": \"object\"}}"
    format = "json"
    content_field = "$.results[*]"
    page_type = "Cursor"
    cursor_field = "start_cursor"
    cursor_response_field = "$.next_cursor"
    schema = {
      fields {
        id = string
        object = string
        created_time = string
        last_edited_time = string
        archived = boolean
      }
    }
  }
}
```

### Extract Fields With JSONPath

```hocon
source {
  Notion {
    url = "https://api.notion.com/v1/users"
    password = "<notion-integration-token>"
    version = "2022-06-28"
    method = "GET"
    format = "json"
    json_field = {
      id = "$.results[*].id"
      type = "$.results[*].type"
      name = "$.results[*].name"
    }
    schema = {
      fields {
        id = string
        type = string
        name = string
      }
    }
  }
}
```

## Changelog

<ChangeLog />