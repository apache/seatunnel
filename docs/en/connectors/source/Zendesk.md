import ChangeLog from '../changelog/connector-http-zendesk.md';

# Zendesk

> Zendesk source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Used to read data from the [Zendesk REST API](https://developer.zendesk.com/api-reference/). It
authenticates with a Zendesk account email and API token (sent as an HTTP Basic `Authorization`
header) and reads a Zendesk endpoint such as tickets, users, or organizations into SeaTunnel rows.

This connector is built on top of the [Http source](Http.md) connector and inherits most of its
options. The differences are the required authentication options and the `format` default.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

|            name             |  type   | required | default value | description                                                                                                                                                                                                                            |
|-----------------------------|---------|----------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                         | String  | Yes      | -             | The Zendesk REST API endpoint to read from, for example `https://your-subdomain.zendesk.com/api/v2/tickets.json`.                                                                                                                       |
| email                       | String  | Yes      | -             | The Zendesk account email used for API token authentication. It is combined with `api_token` as `{email}/token:{api_token}` and sent as an HTTP Basic `Authorization` header.                                                          |
| api_token                   | String  | Yes      | -             | The Zendesk API token. See the [Zendesk API token docs](https://support.zendesk.com/hc/en-us/articles/4408889192858) for how to generate one.                                                                                            |
| method                      | String  | No       | get           | HTTP request method. Only `GET` and `POST` are supported.                                                                                                                                                                              |
| schema                      | Config  | No       | -             | The structure of the data, including field names and field types. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md).                                                                   |
| format                      | String  | No       | text          | The format of upstream data, only `json` and `text` are supported.                                                                                                                                                                      |
| params                      | Map     | No       | -             | Query parameters appended to the request URL.                                                                                                                                                                                           |
| body                        | String  | No       | -             | Request body sent for `POST` (or any method that accepts a body). When `format = "json"`, the body must be valid JSON.                                                                                                                  |
| json_field                  | Config  | No       | -             | Maps JSON paths in the response to schema fields. Must be used together with `schema`. See the [Http source](./Http.md) connector for details and examples.                                                                              |
| content_field               | String  | No       | -             | Extracts a sub-section of the JSON response (for example the array under a top-level key such as `tickets` or `users`) before mapping to rows. See the [Http source](./Http.md) connector for details and examples.                      |
| poll_interval_millis        | int     | No       | -             | Interval in milliseconds between two consecutive requests when running in streaming mode.                                                                                                                                              |
| retry                       | int     | No       | -             | Maximum retry times when the HTTP request throws `IOException`.                                                                                                                                                                        |
| retry_backoff_multiplier_ms | int     | No       | 100           | Retry backoff multiplier in milliseconds.                                                                                                                                                                                               |
| retry_backoff_max_ms        | int     | No       | 10000         | Maximum retry backoff in milliseconds.                                                                                                                                                                                                  |
| enable_multi_lines          | boolean | No       | false         | Whether to parse the response as multiple JSON objects separated by newlines. Only takes effect when `format = "json"`.                                                                                                                 |
| common-options              | config  | No       | -             | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.                                                                                                      |

### url [String]

The Zendesk REST API endpoint to read from, for example
`https://your-subdomain.zendesk.com/api/v2/tickets.json`.

### email [String]

The Zendesk account email used for API token authentication. It is combined with `api_token` as
`{email}/token:{api_token}` and sent as an HTTP Basic `Authorization` header.

### api_token [String]

The Zendesk API token. See the [Zendesk API token docs](https://support.zendesk.com/hc/en-us/articles/4408889192858)
for how to generate one.

### method [String]

HTTP request method. Only `GET` and `POST` are supported. `POST` is typically used together with
`body` for paginated or query-driven Zendesk endpoints.

### schema [Config]

The structure of the data, including field names and field types. For more details, please refer to
[Schema Feature](../../introduction/concepts/schema-feature.md).

### format [String]

The format of upstream data. Only `json` and `text` are supported; default is `text`. Zendesk
endpoints always return JSON, so set `format = "json"` together with `content_field` to extract
the result array before mapping to rows.

### params [Map]

Query parameters appended to the request URL. Use this for Zendesk endpoints that accept filters,
pagination, or other query parameters.

### body [String]

Request body sent for `POST` (or any method that accepts a body). When `format = "json"`, the body
must be valid JSON.

### json_field [Config]

This parameter helps you configure the schema, so this parameter must be used with `schema`. It maps
JSON paths in the response to schema fields. See the [Http source](./Http.md) connector for details
and examples.

### content_field [String]

This parameter extracts a sub-section of the JSON response (for example the array under a top-level
key such as `tickets` or `users`) before mapping to rows. Use `content_field = "$.tickets.*"` when
reading `/api/v2/tickets.json`, since the ticket rows live under the `tickets` key. See the
[Http source](./Http.md) connector for details and examples.

### poll_interval_millis [int]

Interval in milliseconds between two consecutive requests when the connector runs in streaming mode.
Has no effect in batch mode.

### retry [int]

Maximum retry times when the HTTP request throws `IOException`. The retry loop uses
`retry_backoff_multiplier_ms` and `retry_backoff_max_ms` to compute the wait between attempts.

### retry_backoff_multiplier_ms [int]

Base unit (in milliseconds) for the retry backoff. The wait between attempts grows across retries
up to `retry_backoff_max_ms`. The growth curve is not a fixed multiplier per attempt — see
`HttpClientProvider` (`connector-http-base`) for the exact Fibonacci-based strategy.

### retry_backoff_max_ms [int]

Maximum wait between retries, in milliseconds.

### enable_multi_lines [boolean]

Whether to parse the response as multiple JSON objects separated by newlines. Only takes effect when
`format = "json"`.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Task Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Zendesk {
    url = "https://your-subdomain.zendesk.com/api/v2/tickets.json"
    email = "agent@example.com"
    api_token = "${ZENDESK_API_TOKEN}"
    method = "GET"
    format = "json"
    content_field = "$.tickets.*"
    schema = {
      fields {
        id = bigint
        subject = string
        status = string
        priority = string
        created_at = string
        updated_at = string
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
