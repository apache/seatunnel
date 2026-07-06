import ChangeLog from '../changelog/connector-http-onesignal.md';

# OneSignal

> OneSignal source connector

## Description

The OneSignal source connector reads data from OneSignal HTTP APIs. It is built on the HTTP source connector and automatically adds the OneSignal authentication headers from `password`.

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
| OneSignal  | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-http-onesignal) |

## Source Options

| Name                          | Type    | Required | Default | Description |
|-------------------------------|---------|----------|---------|-------------|
| url                           | String  | Yes      | -       | OneSignal API request URL. |
| password                      | String  | Yes      | -       | OneSignal user auth key. The connector writes it to the `Authorization: Basic ...` header and also sets `Content-Type: application/json`. |
| method                        | String  | No       | GET     | HTTP request method. `GET` and `POST` are supported. |
| schema                        | Config  | No       | -       | SeaTunnel schema. Required when `format = json`. |
| schema.fields                 | Config  | No       | -       | Output field names and types. |
| format                        | String  | No       | text    | Response format. Supports `json`, `text`, and `binary`. OneSignal API reads normally use `json`. |
| content_field                 | String  | No       | -       | JSONPath used to extract a JSON object or array before parsing it with `schema`. |
| json_field                    | Config  | No       | -       | JSONPath mapping for individual output fields. Use it together with `schema`. |
| headers                       | Map     | No       | -       | Extra HTTP headers. Authentication headers from `password` are added by the connector. |
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
- Use `content_field` when the OneSignal response wraps the records inside a JSON array or object.
- This connector is source-only. It does not provide a sink connector, CDC, multi-table split discovery, or exactly-once guarantees.
- Do not put secrets directly in shared configuration files. Use your deployment platform's secret management when possible.

## Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  OneSignal {
    plugin_output = "onesignal_apps"
    url = "https://onesignal.com/api/v1/apps"
    password = "YOUR_ONESIGNAL_USER_AUTH_KEY"
    method = "GET"
    format = "json"
    schema = {
      fields {
        id = string
        name = string
        gcm_key = string
        chrome_key = string
        created_at = string
        updated_at = string
        players = int
        messageable_players = int
        basic_auth_key = string
      }
    }
  }
}

sink {
  Console {
    plugin_input = "onesignal_apps"
  }
}
```

## Changelog

<ChangeLog />
