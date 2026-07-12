import ChangeLog from '../changelog/connector-http-github.md';

# Github

> Github source connector

## Description

The Github source connector reads data from the GitHub REST API. It is built on the HTTP source connector, and automatically adds the `Authorization: Bearer <access_token>` request header.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
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

GitHub REST API URL, for example `https://api.github.com/orgs/apache/repos`.

### access_token [String]

GitHub personal access token. The connector sends it as a Bearer token in the HTTP `Authorization` header.

### method [String]

HTTP request method. The common GitHub read scenario uses `GET`.

### headers [Map]

Extra HTTP headers. Do not put `Authorization` here unless you intentionally want to override the header generated from `access_token`.

### params [Map]

HTTP query parameters, such as `per_page`, `page`, `since`, or other GitHub API parameters.

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

Request interval in milliseconds for streaming jobs. In batch jobs the connector reads once and finishes.

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

Read repositories from a GitHub organization:

```hocon
source {
  Github {
    url = "https://api.github.com/orgs/apache/repos"
    access_token = "ghp_xxxxxxxxxxxx"
    method = "GET"
    format = "json"
    schema = {
      fields {
        id = int
        name = string
        description = string
        html_url = string
        stargazers_count = int
        forks = int
      }
    }
  }
}
```

Read paged GitHub API results:

```hocon
source {
  Github {
    url = "https://api.github.com/orgs/apache/repos"
    access_token = "ghp_xxxxxxxxxxxx"
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
        html_url = string
      }
    }
  }
}
```

## Changelog

<ChangeLog />
