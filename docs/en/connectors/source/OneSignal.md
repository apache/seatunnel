import ChangeLog from '../changelog/connector-http-onesignal.md';

# OneSignal

> OneSignal source connector

## Description

The OneSignal source connector reads data from the OneSignal REST API. It is built on the HTTP source connector and automatically sends `password` to OneSignal as the `Authorization: Basic <token>` request header, so you do not need to set `Authorization` yourself.

Use this connector to ingest OneSignal resources such as apps, players, segments, or notifications as SeaTunnel rows.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Source Options

| Name                        | Type    | Required | Default | Description |
|-----------------------------|---------|----------|---------|-------------|
| url                         | String  | Yes      | -       | OneSignal REST API endpoint. Common endpoints include `https://onesignal.com/api/v1/apps` and `https://onesignal.com/api/v1/players`. |
| password                    | String  | Yes      | -       | OneSignal user auth key. The connector sends it as the HTTP `Authorization: Basic <password>` header. Create one at [OneSignal Accounts and Keys](https://documentation.onesignal.com/docs/accounts-and-keys#user-auth-key). |
| method                      | String  | No       | get     | HTTP request method. Supported values are `GET` and `POST`. |
| headers                     | Map     | No       | -       | Extra HTTP headers. Do not put `Authorization` here unless you want to override the header generated from `password`. |
| params                      | Map     | No       | -       | HTTP query parameters, such as `limit`, `offset`, or other OneSignal API parameters. |
| body                        | String  | No       | -       | HTTP request body. Useful for endpoints that accept a JSON payload. |
| format                      | String  | No       | json    | Response format. Use `json` with `schema` to read OneSignal JSON as SeaTunnel rows with named fields. Use `text` to keep the raw response. |
| schema                      | Config  | No       | -       | Output row structure. Required when `format = "json"`. See [Schema Feature](../../introduction/concepts/schema-feature.md). |
| schema.fields               | Config  | No       | -       | Field names and SeaTunnel data types used to parse the JSON response. |
| json_field                  | Config  | No       | -       | Field-level JSONPath mapping. Use it with `schema` when each output field lives at a different JSON path. |
| content_field               | String  | No       | -       | JSONPath expression that selects a JSON fragment before `schema` parses it. For example, use `$.players[*]` to flatten a list response. |
| pageing                     | Config  | No       | -       | HTTP pagination settings inherited from the HTTP source connector. OneSignal paged endpoints use `page` / `per_page` parameters. |
| poll_interval_millis        | Int     | No       | -       | Request interval in milliseconds for streaming jobs. In batch mode the connector reads once and finishes. |
| retry                       | Int     | No       | -       | Maximum retry count when an HTTP request fails with `IOException`. |
| retry_backoff_multiplier_ms | Int     | No       | 100     | Retry backoff multiplier in milliseconds. |
| retry_backoff_max_ms        | Int     | No       | 10000   | Maximum retry backoff in milliseconds. |
| enable_multi_lines          | Boolean | No       | false   | When `true`, multiple JSON objects separated by newlines in the response body are treated as separate records. |
| json_filed_missed_return_null | Boolean | No    | false   | When `true`, missing JSON fields return `null`; otherwise a missing field causes an error. |
| common-options              | Config  | No       | -       | Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md). |

## Usage Notes

- `password` is sensitive. Avoid hardcoding real keys in shared job files. Use SeaTunnel variable substitution or your deployment secret mechanism.
- The connector always adds an `Authorization` header from `password`. Put other custom headers in `headers`.
- Set `format = "json"` and define `schema` when you want typed SeaTunnel rows.
- Use `content_field` when OneSignal wraps records in a nested array such as `$.players[*]`.
- Use `json_field` only when each output field needs its own JSONPath expression.
- OneSignal paged endpoints accept `page` and `per_page` query parameters; configure them through `params` and `pageing`.

## Task Examples

### Read Apps

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  OneSignal {
    url = "https://onesignal.com/api/v1/apps"
    password = "<onesignal-user-auth-key>"
    method = "GET"
    format = "json"
    schema = {
      fields {
        id = string
        name = string
        gcm_key = string
        chrome_key = string
        site_name = string
        created_at = string
        updated_at = string
        players = int
        messageable_players = int
      }
    }
  }
}

sink {
  Console {
  }
}
```

### Read Players With Pagination

Use `params` together with `pageing` to walk through paged OneSignal endpoints:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  OneSignal {
    url = "https://onesignal.com/api/v1/players"
    password = "<onesignal-user-auth-key>"
    method = "GET"
    params = {
      app_id = "<your-app-id>"
      limit = "50"
      offset = "0"
    }
    pageing = {
      page_field = "offset"
      start_page_number = 0
      page_step = 50
      total_page_size = 10
      use_placeholder_replacement = false
    }
    format = "json"
    content_field = "$.players[*]"
    schema = {
      fields {
        id = string
        identifier = string
        device_type = int
        sessions = int
        language = string
        game_version = string
      }
    }
  }
}
```

### Extract Fields With JSONPath

Use `json_field` when each output field lives at a different JSON path:

```hocon
source {
  OneSignal {
    url = "https://onesignal.com/api/v1/apps"
    password = "<onesignal-user-auth-key>"
    method = "GET"
    format = "json"
    json_field = {
      id = "$.id"
      name = "$.name"
      players = "$.players"
      site_name = "$.site_name"
    }
    schema = {
      fields {
        id = string
        name = string
        players = int
        site_name = string
      }
    }
  }
}
```

## Changelog

<ChangeLog />