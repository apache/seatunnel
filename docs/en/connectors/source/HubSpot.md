import ChangeLog from '../changelog/connector-http-hubspot.md';

# HubSpot

> HubSpot Source Connector

## Description

The HubSpot source connector reads data from the HubSpot CRM V3 REST API. It is
built on the shared HTTP source connector and adds HubSpot-specific defaults for:

- `Authorization: Bearer <access_token>` request headers
- automatic URL construction from `object_type`
- JSON response parsing
- cursor pagination based on HubSpot's `paging.next.after` response field

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

:::tip

HubSpot inherits the shared HTTP source runtime. Batch jobs finish after one scan,
while streaming jobs keep polling based on `poll_interval_millis`. Only
`format = "binary"` stays batch-only because it follows the shared HTTP binary
contract.

:::

## Options

| name                           | type    | required | default value |
|--------------------------------|---------|----------|---------------|
| access_token                   | String  | Yes      | -             |
| object_type                    | String  | No       | contacts      |
| url                            | String  | No       | derived from `object_type` |
| method                         | String  | No       | GET           |
| headers                        | Map     | No       | -             |
| params                         | Map     | No       | -             |
| body                           | String  | No       | -             |
| format                         | String  | No       | JSON          |
| schema                         | Config  | No       | -             |
| json_field                     | Config  | No       | -             |
| content_field                  | String  | No       | `$.results`   |
| pageing                        | Config  | No       | cursor paging defaults |
| binary_chunk_size              | long    | No       | 10485760      |
| poll_interval_millis           | int     | No       | -             |
| retry                          | int     | No       | -             |
| retry_backoff_multiplier_ms    | int     | No       | 100           |
| retry_backoff_max_ms           | int     | No       | 10000         |
| enable_multi_lines             | boolean | No       | false         |
| connect_timeout_ms             | int     | No       | 12000         |
| socket_timeout_ms              | int     | No       | 60000         |
| keep_params_as_form            | boolean | No       | false         |
| keep_page_param_as_http_param  | boolean | No       | true          |
| json_filed_missed_return_null  | boolean | No       | false         |

### access_token [String]

HubSpot private app access token. The connector sends it as a Bearer token in the
HTTP `Authorization` header.

### object_type [String]

The HubSpot CRM object to fetch. Common values include `contacts`, `companies`,
`deals`, `products`, `tickets`, and `quotes`.

### url [String]

Optional HubSpot API URL override. If it is not provided, the connector derives
`https://api.hubapi.com/crm/v3/objects/{object_type}` automatically.

### method [String]

HTTP request method. The common HubSpot read path uses `GET`.

### headers [Map]

Extra HTTP headers. If `Authorization` is not provided, the connector injects
`Authorization: Bearer <access_token>` automatically. Provide
`headers.Authorization` only when you intentionally need to override that value.

### params [Map]

HTTP query parameters. This is useful when you need to add filters, page size, or
other HubSpot API query parameters.

### body [String]

HTTP request body. This is only useful for HubSpot endpoints that accept a request body.

### format [String]

Response format. Supports `json`, `text`, and `binary`. HubSpot defaults to `JSON`
because the connector expects JSON CRM API responses when `format` is not provided.

### schema [Config]

Defines the output row structure when `format = "JSON"`. For details, see
[Schema Feature](../../introduction/concepts/schema-feature.md).

Nested field definitions live under `schema.fields`.

### json_field [Config]

Maps output fields to JSONPath expressions. Use it with `schema` when the required
values are nested in the HubSpot response payload.

### content_field [String]

JSONPath expression used to select the response fragment before schema parsing.
HubSpot defaults to `$.results`.

### pageing [Config]

Pagination settings inherited from the HTTP connector. HubSpot defaults to cursor
paging with `after` and reads the next cursor from `$.paging.next.after` for
JSON and text responses. Binary mode does not support pagination. Keep the option
name `pageing` in job configs.

Common `pageing` fields for HubSpot:

| name | type | required | default value | description |
|------|------|----------|---------------|-------------|
| page_type | String | No | Cursor | Pagination type. HubSpot uses cursor pagination by default. |
| cursor_field | String | No | `after` | Request parameter name for the cursor token. |
| cursor_response_field | String | No | `$.paging.next.after` | JSONPath used to read the next cursor from the response. |
| use_placeholder_replacement | boolean | No | false | Use `${field}` placeholder replacement in headers, parameters, and body. |
| total_page_size | long | No | 0 | Inherited page limit setting for shared HTTP pagination. |
| batch_size | int | No | 100 | Inherited page size used by page-number pagination. |
| start_page_number | long | No | 1 | Inherited first page number for page-number pagination. |
| page_field | String | No | `page` | Inherited request parameter name for page-number pagination. |

### binary_chunk_size [long]

Chunk size in bytes when `format = "binary"`. This only applies to batch jobs and
follows the shared HTTP binary reader behavior.

### poll_interval_millis [int]

Request interval in milliseconds for streaming jobs. In batch jobs the connector
reads once and finishes.

### retry [int]

Maximum retry count when an HTTP request fails with `IOException`.

### retry_backoff_multiplier_ms [int]

Retry backoff multiplier in milliseconds.

### retry_backoff_max_ms [int]

Maximum retry backoff in milliseconds.

### enable_multi_lines [boolean]

When `true`, split text responses line by line in the shared HTTP reader.

### connect_timeout_ms [int]

HTTP connection timeout in milliseconds. Default is `12000`.

### socket_timeout_ms [int]

HTTP socket timeout in milliseconds. Default is `60000`.

### keep_params_as_form [boolean]

When `true`, submit `params` as form fields for compatibility with APIs that expect
form-style requests.

### keep_page_param_as_http_param [boolean]

When `true`, the connector injects the generated paging parameters directly into the
HTTP query params. HubSpot enables this behavior by default so cursor pagination
works without extra request templating.

### json_filed_missed_return_null [boolean]

When `true`, missing JSON fields return `null`; otherwise a missing field causes an error.

### common options

Source plugin common parameters. See
[Source Common Options](../common-options/source-common-options.md).

## Example

Read HubSpot contacts with the default CRM V3 URL:

```hocon
source {
  HubSpot {
    access_token = "pat-na1-..."
    object_type = "contacts"
    format = "JSON"
    schema = {
      fields {
        id = string
        properties = string
      }
    }
    json_field = {
      id = "$.id"
      properties = "$.properties"
    }
  }
}
```

Read HubSpot data from a custom URL with explicit pagination parameters:

```hocon
source {
  HubSpot {
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    access_token = "pat-na1-..."
    params = {
      limit = "100"
    }
    pageing = {
      page_type = "Cursor"
      cursor_field = "after"
      cursor_response_field = "$.paging.next.after"
    }
    format = "JSON"
    schema = {
      fields {
        id = string
      }
    }
    json_field = {
      id = "$.id"
    }
  }
}
```

## Changelog

<ChangeLog />
