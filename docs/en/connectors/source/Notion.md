import ChangeLog from '../changelog/connector-http-notion.md';

# Notion

> Notion source connector

## Description

The Notion source connector reads data from Notion HTTP APIs. It is built on the HTTP source connector and automatically adds the Notion `Authorization` and `Notion-Version` headers from `password` and `version`.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Supported Data Source Info

| Datasource | Dependency |
|------------|------------|
| Notion     | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-http-notion) |

## Source Options

| Name                          | Type    | Required | Default | Description |
|-------------------------------|---------|----------|---------|-------------|
| url                           | String  | Yes      | -       | Notion API request URL. |
| password                      | String  | Yes      | -       | Notion integration token. The connector writes it to the `Authorization: Bearer ...` header. |
| version                       | String  | Yes      | -       | Notion API version, for example `2022-06-28`. The connector writes it to the `Notion-Version` header. |
| method                        | String  | No       | GET     | HTTP request method. `GET` and `POST` are supported. |
| schema                        | Config  | No       | -       | SeaTunnel schema. Required when `format = json`. |
| schema.fields                 | Config  | No       | -       | Output field names and types. |
| format                        | String  | No       | text    | Response format. Supports `json`, `text`, and `binary`. Notion API reads normally use `json`. |
| content_field                 | String  | No       | -       | JSONPath used to extract a JSON object or array before parsing it with `schema`. |
| json_field                    | Config  | No       | -       | JSONPath mapping for individual output fields. Use it together with `schema`. |
| headers                       | Map     | No       | -       | Extra HTTP headers. Notion authentication headers from `password` and `version` are added by the connector. |
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
- Notion list APIs commonly return records inside `results`. Use `content_field = "$.results.*"` to parse each returned object as one SeaTunnel row.
- Nested objects can be described in `schema.fields`, for example `person = { email = string }`.
- This connector is source-only. It does not provide a sink connector, CDC, multi-table split discovery, or exactly-once guarantees.
- Do not put secrets directly in shared configuration files. Use your deployment platform's secret management when possible.

## Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Notion {
    plugin_output = "notion_users"
    url = "https://api.notion.com/v1/users"
    password = "YOUR_NOTION_TOKEN"
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
    plugin_input = "notion_users"
  }
}
```

## Changelog

<ChangeLog />
