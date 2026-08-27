import ChangeLog from '../changelog/connector-http-gitlab.md';

# Gitlab

> Gitlab source connector

## Description

The Gitlab source connector reads data from the GitLab REST API. It is built on the HTTP source connector, and automatically sends `access_token` as the GitLab `PRIVATE-TOKEN` request header.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
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
| schema                      | Config  | No       | -             |
| schema.fields               | Config  | No       | -             |
| json_field                  | Config  | No       | -             |
| content_field               | String  | No       | -             |
| pageing                     | Config  | No       | -             |
| page_type                   | String  | No       | PageNumber    |
| cursor_field                | String  | No       | -             |
| cursor_response_field       | String  | No       | -             |
| poll_interval_millis        | int     | No       | -             |
| retry                       | int     | No       | -             |
| retry_backoff_multiplier_ms | int     | No       | 100           |
| retry_backoff_max_ms        | int     | No       | 10000         |
| enable_multi_lines          | boolean | No       | false         |
| keep_params_as_form         | boolean | No       | false         |
| keep_page_param_as_http_param | boolean | No    | false         |
| batch_size                  | int     | No       | 100           |
| start_page_number           | long    | No       | 1             |
| total_page_size             | long    | No       | 0             |
| use_placeholder_replacement | boolean | No       | false         |
| connect_timeout_ms          | int     | No       | 12000         |
| socket_timeout_ms           | int     | No       | 60000         |
| json_filed_missed_return_null | boolean | No     | false         |
| common-options              | config  | No       | -             |

### url [String]

GitLab REST API URL, for example `https://gitlab.com/api/v4/projects`.

### access_token [String]

GitLab personal access token. The connector sends it in the HTTP `PRIVATE-TOKEN` header.

### method [String]

HTTP request method. The common GitLab read scenario uses `GET`.

### headers [Map]

Extra HTTP headers. Do not put `PRIVATE-TOKEN` here unless you intentionally want to override the header generated from `access_token`.

### params [Map]

HTTP query parameters, such as `per_page`, `page`, `owned`, or other GitLab API parameters.

### body [String]

HTTP request body. This is only useful for API endpoints that accept a request body.

### format [String]

Response format. Supports `json` and `text`. Use `json` with `schema` when you want SeaTunnel rows with named fields.

### schema [Config]

Defines the output row structure when `format = "json"`. For details, see [Schema Feature](../../introduction/concepts/schema-feature.md).

### json_field [Config]

Maps output fields to JSONPath expressions. Use it with `schema` when the required values are nested in the response.

### content_field [String]

JSONPath expression used to select a JSON fragment before schema parsing, for example `$.items[*]`.

### pageing [Config]

Pagination settings inherited from the HTTP connector. Keep the option name `pageing` in job configs.

### page_type [String]

Pagination type. Supported values are `PageNumber` (default) and `Cursor`. Use `Cursor` for GitLab endpoints that return a `X-Next-Page` cursor header.

### cursor_field [String]

The request parameter name that carries the cursor value. Used together with `page_type = "Cursor"`.

### cursor_response_field [String]

The JSONPath of the cursor in the response body. Used together with `page_type = "Cursor"`.

### poll_interval_millis [int]

This option is inherited from the HTTP connector, but Gitlab source currently supports batch mode only.

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

### batch_size [int]

The number of records returned per page request when the total number of pages is unknown.

### start_page_number [long]

Which page number to start synchronizing from.

### total_page_size [long]

Total page size to read. `0` means use `batch_size` until the API stops returning new pages.

### use_placeholder_replacement [boolean]

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
- The connector always adds a `PRIVATE-TOKEN` header from `access_token`. Put other custom headers in `headers`.
- Set `format = "json"` and define `schema` when you want typed SeaTunnel rows.
- Use `content_field` when the GitLab response wraps records in a nested array such as `$.items[*]`.
- For traditional page-number pagination, keep `page_type = "PageNumber"` and use `params` with `page` / `per_page`.
- The GitLab source supports batch mode only; `poll_interval_millis` does not enable streaming behavior.

## Task Examples

### Read Projects From GitLab

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Gitlab {
    url = "https://gitlab.com/api/v4/projects"
    access_token = "glpat-xxxxxxxxxxxx"
    method = "GET"
    format = "json"
    schema = {
      fields {
        id = int
        description = string
        name = string
        name_with_namespace = string
        path = string
        http_url_to_repo = string
      }
    }
  }
}

sink {
  Console {
  }
}
```

### Read Paged GitLab API Results

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Gitlab {
    url = "https://gitlab.com/api/v4/projects"
    access_token = "glpat-xxxxxxxxxxxx"
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
        path = string
      }
    }
  }
}
```

### Filter And Extract Fields With JSONPath

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Gitlab {
    url = "https://gitlab.com/api/v4/projects"
    access_token = "glpat-xxxxxxxxxxxx"
    method = "GET"
    params = {
      owned = "true"
      per_page = "50"
    }
    format = "json"
    content_field = "$.[*]"
    json_field = {
      id = "$.id"
      name = "$.name"
      visibility = "$.visibility"
    }
    schema = {
      fields {
        id = int
        name = string
        visibility = string
      }
    }
  }
}
```

## Changelog

<ChangeLog />