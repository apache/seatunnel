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

## Options

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
| poll_interval_millis        | int     | No       | -             |
| retry                       | int     | No       | -             |
| retry_backoff_multiplier_ms | int     | No       | 100           |
| retry_backoff_max_ms        | int     | No       | 10000         |
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

### poll_interval_millis [int]

This option is inherited from the HTTP connector, but Gitlab source currently supports batch mode only.

### retry [int]

Maximum retry count when an HTTP request fails with `IOException`.

### retry_backoff_multiplier_ms [int]

Retry backoff multiplier in milliseconds.

### retry_backoff_max_ms [int]

Maximum retry backoff in milliseconds.

### json_filed_missed_return_null [boolean]

When `true`, missing JSON fields return `null`; otherwise a missing field causes an error.

### common options

Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md).

## Example

Read projects from GitLab:

```hocon
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
```

Read paged GitLab API results:

```hocon
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

## Changelog

<ChangeLog />
