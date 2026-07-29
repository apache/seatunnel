import ChangeLog from '../changelog/connector-http-zendesk.md';

# Zendesk

> Zendesk source connector

## Description

Used to read data from the [Zendesk REST API](https://developer.zendesk.com/api-reference/). It authenticates with a Zendesk account email and API token (sent as an HTTP Basic `Authorization` header) and reads a Zendesk endpoint such as tickets, users, or organizations into SeaTunnel rows.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

|            name             |  type   | required | default value |
|-----------------------------|---------|----------|---------------|
| url                         | String  | Yes      | -             |
| email                       | String  | Yes      | -             |
| api_token                   | String  | Yes      | -             |
| method                      | String  | No       | get           |
| schema                      | Config  | No       | -             |
| format                      | String  | No       | text          |
| params                      | Map     | No       | -             |
| body                        | String  | No       | -             |
| json_field                  | Config  | No       | -             |
| content_field                | String  | No       | -             |
| poll_interval_millis        | int     | No       | -             |
| retry                       | int     | No       | -             |
| retry_backoff_multiplier_ms | int     | No       | 100           |
| retry_backoff_max_ms        | int     | No       | 10000         |
| enable_multi_lines          | boolean | No       | false         |
| common-options              | config  | No       | -             |

### url [String]

The Zendesk REST API endpoint to read from, for example `https://your-subdomain.zendesk.com/api/v2/tickets.json`.

### email [String]

The Zendesk account email used for API token authentication. It is combined with `api_token` as `{email}/token:{api_token}` and sent as an HTTP Basic `Authorization` header.

### api_token [String]

The Zendesk API token. See the [Zendesk API token docs](https://support.zendesk.com/hc/en-us/articles/4408889192858) for how to generate one.

### method [String]

http request method, only supports GET, POST method.

### schema [Config]

The structure of the data, including field names and field types. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md).

### format [String]

the format of upstream data, now only support `json` `text`, default `text`.

### params [Map]

http params

### json_field [Config]

This parameter helps you configure the schema, so this parameter must be used with schema. It maps JSON paths in the response to schema fields. See the [Http source](./Http.md) connector for details and examples.

### content_field [String]

This parameter can extract a sub-section of the JSON response (for example the array under a top-level key such as `tickets` or `users`) before mapping to rows. See the [Http source](./Http.md) connector for details and examples.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Example

```hocon
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
```

## Changelog

<ChangeLog />
