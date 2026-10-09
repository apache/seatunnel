import ChangeLog from '../changelog/connector-http-linear.md';

# Http-Linear

> Linear source connector

## Description

The Linear source connector reads data from the [Linear](https://linear.app/) GraphQL API. It is based on the HTTP source connector and sends the configured `api_key` as the `Authorization` request header, as required by the Linear API.

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
| url                         | String  | Yes      | -       | Linear API endpoint URL, usually `https://api.linear.app/graphql`. |
| api_key                     | String  | Yes      | -       | Linear API key. The connector sends it as the `Authorization` request header. |
| method                      | String  | No       | get     | HTTP request method. Supported values are `GET` and `POST`. |
| headers                     | Map     | No       | -       | Extra HTTP headers. `Authorization` is set automatically from `api_key`. |
| params                      | Map     | No       | -       | Query parameters sent with the request. |
| body                        | String  | No       | -       | HTTP request body. Usually used with `method = "POST"` to send a GraphQL query. |
| format                      | String  | No       | text    | Response format. Use `json` when reading the GraphQL response into a SeaTunnel schema; use `text` to return the raw response as `content`. |
| schema                      | Config  | No       | -       | Output schema. Required when `format = "json"`. |
| schema.fields               | Config  | No       | -       | Field names and SeaTunnel data types used to parse the JSON response. |
| content_field               | String  | No       | -       | JSONPath used to select a nested part of the response before parsing it with `schema`, for example `$.data.issues.nodes.*`. |
| json_field                  | Config  | No       | -       | Field-level JSONPath mapping. Use it with `schema` when output fields come from different JSON paths. |
| pageing                     | Config  | No       | -       | HTTP pagination settings inherited from the HTTP source connector. |
| page_type                   | String  | No       | PageNumber | Pagination type. Supported values are `PageNumber` (default) and `Cursor`. Use `Cursor` for Linear connections that paginate with `page_info` / `endCursor`. |
| cursor_field                | String  | No       | -       | The request parameter name that carries the cursor value. Used together with `page_type = "Cursor"`. |
| cursor_response_field       | String  | No       | -       | The JSONPath of the cursor in the response body. Used together with `page_type = "Cursor"`. |
| poll_interval_millis        | Int     | No       | -       | Request interval in milliseconds when the source is used in streaming mode. The Linear source supports batch mode only. |
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
| connect_timeout_ms          | Int     | No       | 12000   | HTTP connection timeout in milliseconds. |
| socket_timeout_ms           | Int     | No       | 60000   | HTTP socket timeout in milliseconds. |
| json_filed_missed_return_null | Boolean | No    | false   | Return null when a configured JSON field is missing. |
| common-options              | Config  | No       | -       | Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md). |

:::tip

`api_key` is a sensitive credential. Avoid hardcoding real keys in shared job files. Use SeaTunnel variable substitution or your deployment secret mechanism when possible.

:::

## Usage Notes

- The plugin name in the job configuration is `Linear`.
- Set `format = "json"` and configure `schema` when you want typed SeaTunnel rows.
- Use `content_field` to unwrap the GraphQL envelope, for example `$.data.issues.nodes.*`, before parsing with `schema`.
- Use `json_field` only when each output field needs its own JSONPath expression.
- `api_key` overrides the `Authorization` header. Put only other custom headers in `headers`.

## Task Examples

### Read Issues

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Linear {
    url = "https://api.linear.app/graphql"
    api_key = "<your_linear_api_key>"
    method = "POST"
    body = "{\"query\": \"{ issues { nodes { id title } } }\"}"
    format = "json"
    content_field = "$.data.issues.nodes.*"
    schema = {
      fields {
        id = string
        title = string
      }
    }
    plugin_output = "linear_data"
  }
}

sink {
  Console {
  }
}
```

### Extract Fields With JSONPath

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Linear {
    url = "https://api.linear.app/graphql"
    api_key = "<your_linear_api_key>"
    method = "POST"
    body = "{\"query\": \"{ issues { nodes { id title } } }\"}"
    format = "json"
    json_field = {
      id = "$.data.issues.nodes[*].id"
      title = "$.data.issues.nodes[*].title"
    }
    schema = {
      fields {
        id = string
        title = string
      }
    }
  }
}
```

## Changelog

<ChangeLog />
