import ChangeLog from '../changelog/connector-http-github.md';

# Github

> Github source connector

## Description

The Github source connector reads data from the GitHub REST API. It is built on the HTTP source connector, and automatically adds the `Authorization: Bearer <access_token>` request header.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Source Options

| name                        | type    | required | default value |
|-----------------------------|---------|----------|---------------|
| url                         | String  | Yes      | -             |
| access_token                | String  | Yes      | -             |
| method                      | String  | No       | GET           |
| headers                     | Map     | No       | -             |
| params                      | Map     | No       | -             |
| body                        | String  | No       | -             |
| format                      | String  | No       | text          |
| binary_chunk_size           | long    | No       | 10485760      |
| schema                      | Config  | No       | -             |
| schema.fields               | Config  | No       | -             |
| json_field                  | Config  | No       | -             |
| content_field               | String  | No       | -             |
| pageing                     | Config  | No       | -             |
| pageing.page_type           | String  | No       | PageNumber    |
| pageing.cursor_field        | String  | No       | -             |
| pageing.cursor_response_field | String | No      | -             |
| poll_interval_millis        | int     | No       | -             |
| retry                       | int     | No       | -             |
| retry_backoff_multiplier_ms | int     | No       | 100           |
| retry_backoff_max_ms        | int     | No       | 10000         |
| enable_multi_lines          | boolean | No       | false         |
| keep_params_as_form         | boolean | No       | false         |
| keep_page_param_as_http_param | boolean | No    | false         |
| pageing.batch_size          | int     | No       | 100           |
| pageing.start_page_number   | long    | No       | 1             |
| pageing.total_page_size     | long    | No       | 0             |
| pageing.use_placeholder_replacement | boolean | No | false         |
| connect_timeout_ms          | int     | No       | 12000         |
| socket_timeout_ms           | int     | No       | 60000         |
| json_filed_missed_return_null | boolean | No     | false         |
| common-options              | config  | No       | -             |

### url [String]

GitHub REST API URL, for example `https://api.github.com/orgs/apache/repos`.

### access_token [String]

GitHub personal access token. The connector sends it as a Bearer token in the HTTP `Authorization` header.

### method [String]

HTTP request method. The common GitHub read scenario uses `GET`.

### headers [Map]

Extra HTTP headers. Do not put `Authorization` here unless you intentionally want to override the header generated from `access_token`.

### params [Map]

HTTP query parameters, such as `per_page`, `page`, `since`, or other GitHub API parameters.

### body [String]

HTTP request body. This is only useful for API endpoints that accept a request body.

### format [String]

Response format. Supports `json` and `text`. Use `json` with `schema` when you want SeaTunnel rows with named fields.

### binary_chunk_size [long]

Chunk size in bytes when `format = "binary"`. Large responses are split into multiple rows. Only effective in BATCH mode.

### schema [Config]

Defines the output row structure when `format = "json"`. For details, see [Schema Feature](../../introduction/concepts/schema-feature.md).

### json_field [Config]

Maps output fields to JSONPath expressions. Use it with `schema` when the required values are nested in the response.

### content_field [String]

JSONPath expression used to select a JSON fragment before schema parsing, for example `$.items[*]`.

### pageing [Config]

Pagination settings inherited from the HTTP connector. All pagination options must be configured inside this `pageing` block; pagination keys set at the top level of the source are ignored. Set `pageing.page_type = "Cursor"` for cursor-based pagination when needed.

### pageing.page_type [String]

Pagination type. Supported values are `PageNumber` (default) and `Cursor`. When `Cursor` is used, the next cursor is read from the response body via `pageing.cursor_response_field`; HTTP response headers are not read.

### pageing.cursor_field [String]

The request parameter name that carries the cursor value. Used together with `pageing.page_type = "Cursor"`.

### pageing.cursor_response_field [String]

The JSONPath of the cursor in the response body. Used together with `pageing.page_type = "Cursor"`.

### poll_interval_millis [int]

Request interval in milliseconds for streaming jobs. In batch jobs the connector reads once and finishes.

### retry [int]

Maximum retry count when an HTTP request fails with `IOException`.

### retry_backoff_multiplier_ms [int]

Retry backoff multiplier in milliseconds.

### retry_backoff_max_ms [int]

Maximum retry backoff in milliseconds.

### enable_multi_lines [boolean]

When `true`, multiple JSON objects separated by newlines in the response body are treated as separate records.

### keep_params_as_form [boolean]

When `true`, request parameters are sent as form-encoded body parameters instead of URL query parameters.

### keep_page_param_as_http_param [boolean]

When `true`, the page parameter remains in the request URL when paginating instead of being replaced inside the body.

### pageing.batch_size [int]

The number of records returned per page request when the total number of pages is unknown.

### pageing.start_page_number [long]

Which page number to start synchronizing from.

### pageing.total_page_size [long]

Total page size to read. `0` means use `pageing.batch_size` until the API stops returning new pages.

### pageing.use_placeholder_replacement [boolean]

When `true`, use `${field}` placeholder replacement for headers, parameters and body values; otherwise use key-based replacement.

### connect_timeout_ms [int]

HTTP connection timeout in milliseconds. Default 12000ms.

### socket_timeout_ms [int]

HTTP socket timeout in milliseconds. Default 60000ms.

### json_filed_missed_return_null [boolean]

When `true`, missing JSON fields return `null`; otherwise a missing field causes an error.

### common options

Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md).

## Usage Notes

- `access_token` is sensitive. Avoid hardcoding real tokens in shared job files. Use SeaTunnel variable substitution or your deployment secret mechanism.
- The connector always adds an `Authorization: Bearer <access_token>` header from `access_token`. Put other custom headers in `headers`.
- Set `format = "json"` and define `schema` when you want typed SeaTunnel rows.
- Use `content_field` when the GitHub response wraps records in a nested array such as `$.items[*]`.
- For traditional page-number pagination, keep `pageing.page_type = "PageNumber"` (the default) and use `params` with `page` / `per_page`. Pagination options must be nested inside the `pageing` block.
- For cursor-based endpoints, set `pageing.page_type = "Cursor"` and configure `pageing.cursor_field` / `pageing.cursor_response_field` inside the `pageing` block. The next cursor is read from the response body via `pageing.cursor_response_field`.

## Task Examples

### Read Repositories From A GitHub Organization

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Github {
    url = "https://api.github.com/orgs/apache/repos"
    access_token = "ghp_xxxxxxxxxxxx"
    method = "GET"
    format = "json"
    schema = {
      fields {
        id = int
        name = string
        description = string
        html_url = string
        stargazers_count = int
        forks = int
      }
    }
  }
}

sink {
  Console {
  }
}
```

### Read Paged GitHub API Results

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Github {
    url = "https://api.github.com/orgs/apache/repos"
    access_token = "ghp_xxxxxxxxxxxx"
    method = "GET"
    params = {
      per_page = "100"
      page = "${page}"
    }
    pageing = {
      page_field = "page"
      total_page_size = 5
      start_page_number = 1
      use_placeholder_replacement = true
    }
    format = "json"
    schema = {
      fields {
        id = int
        name = string
        html_url = string
      }
    }
  }
}
```

### Stream GitHub Events

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  Github {
    url = "https://api.github.com/orgs/apache/events"
    access_token = "ghp_xxxxxxxxxxxx"
    method = "GET"
    format = "json"
    poll_interval_millis = 60000
    schema = {
      fields {
        id = string
        type = string
        created_at = string
      }
    }
  }
}
```

## Changelog

<ChangeLog />