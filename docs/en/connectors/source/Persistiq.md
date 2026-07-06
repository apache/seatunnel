import ChangeLog from '../changelog/connector-http-persistiq.md';

# Persistiq

> Persistiq source connector

## Description

The Persistiq source connector reads data from Persistiq HTTP APIs. It is built on the HTTP source connector and automatically sends the Persistiq API key from `password` as the `x-api-key` header.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [schema projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Supported Data Source Info

| Datasource | Dependency |
|------------|------------|
| Persistiq  | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-http-persistiq) |

## Source Options

| Name                          | Type    | Required | Default | Description |
|-------------------------------|---------|----------|---------|-------------|
| url                           | String  | Yes      | -       | Persistiq API request URL. |
| password                      | String  | Yes      | -       | Persistiq API key. The connector writes it to the `x-api-key` header. |
| method                        | String  | No       | GET     | HTTP request method. `GET` and `POST` are supported. |
| schema                        | Config  | No       | -       | SeaTunnel schema. Required when `format = json`. |
| schema.fields                 | Config  | No       | -       | Output field names and types. |
| format                        | String  | No       | text    | Response format. Supports `json`, `text`, and `binary`. Persistiq API reads normally use `json`. |
| content_field                 | String  | No       | -       | JSONPath used to extract a JSON object or array before parsing it with `schema`. |
| json_field                    | Config  | No       | -       | JSONPath mapping for individual output fields. Use it together with `schema`. |
| headers                       | Map     | No       | -       | Extra HTTP headers. The API key header from `password` is added by the connector. |
| params                        | Map     | No       | -       | HTTP query parameters. |
| body                          | String  | No       | -       | HTTP request body. Usually used with `method = POST`. |
| pageing                       | Config  | No       | -       | Pagination settings. Keep the option name spelling as `pageing`. |
| poll_interval_millis          | Int     | No       | -       | Request interval in streaming jobs, in milliseconds. |
| retry                         | Int     | No       | -       | Maximum retry count when the request fails with `IOException`. |
| retry_backoff_multiplier_ms   | Int     | No       | 100     | Retry backoff multiplier in milliseconds. |
| retry_backoff_max_ms          | Int     | No       | 10000   | Maximum retry backoff in milliseconds. |
| enable_multi_lines            | Boolean | No       | false   | Whether to split text responses by line. |
| connect_timeout_ms            | Int     | No       | 12000   | HTTP connection timeout in milliseconds. |
| socket_timeout_ms             | Int     | No       | 60000   | HTTP socket timeout in milliseconds. |
| json_filed_missed_return_null | Boolean | No       | false   | When a JSON field is missing, return null instead of failing. Keep the option name spelling as `json_filed_missed_return_null`. |
| common-options                | Config  | No       | -       | Source plugin common options. See [Source Common Options](../common-options/source-common-options.md). |

## Option Notes

- `format = json` requires `schema`; otherwise the connector reads the response as a single `content` text field.
- Persistiq list APIs commonly return records inside a wrapper object. Use `content_field`, for example `content_field = "$.users.*"`, to parse each record as one SeaTunnel row.
- This connector is source-only. It does not provide a sink connector, CDC, multi-table split discovery, or exactly-once guarantees.
- Do not put secrets directly in shared configuration files. Use your deployment platform's secret management when possible.

## Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Persistiq {
    plugin_output = "persistiq_users"
    url = "https://api.persistiq.com/v1/users"
    password = "YOUR_PERSISTIQ_API_KEY"
    method = "GET"
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
  Console {
    plugin_input = "persistiq_users"
  }
}
```

## Changelog

<ChangeLog />
