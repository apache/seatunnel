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
| poll_interval_millis        | Int     | No       | -       | Request interval in milliseconds when the source is used in streaming mode. |
| retry                       | Int     | No       | -       | Maximum retry times when the HTTP request fails with an `IOException`. |
| retry_backoff_multiplier_ms | Int     | No       | 100     | Retry backoff multiplier in milliseconds. |
| retry_backoff_max_ms        | Int     | No       | 10000   | Maximum retry backoff in milliseconds. |
| json_filed_missed_return_null | Boolean | No     | false   | Return null when a configured JSON field is missing. |
| common-options              | Config  | No       | -       | Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md). |

:::tip

`password` is a sensitive Notion integration token. Avoid hardcoding real tokens in shared job files. Use SeaTunnel variable substitution or your deployment secret mechanism when possible.

:::

## Usage Notes

- Set `format = "json"` and configure `schema` when you want typed SeaTunnel rows.
- Use `content_field` when the Notion response wraps records in a nested array, such as `$.results.*`.
- Use `json_field` only when each output field needs its own JSONPath expression.
- `password` and `version` override the `Authorization` and `Notion-Version` headers. Put only other custom headers in `headers`.

## Task Example

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
