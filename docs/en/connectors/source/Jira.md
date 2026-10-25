import ChangeLog from '../changelog/connector-http-jira.md';

# Jira

> Jira source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Reads data from Jira REST APIs. The connector adds the Jira Basic authentication header from `email` and `api_token`, then uses the shared HTTP source runtime to parse the response.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

:::tip

The Jira source is a batch-only connector. It throws an error when the job runs in streaming mode.

:::

## Options

| name                        | type   | required | default value | description |
|-----------------------------|--------|----------|---------------|-------------|
| url                         | String | Yes      | -             | Jira REST API URL. |
| email                       | String | Yes      | -             | Jira account email used for Basic authentication. |
| api_token                   | String | Yes      | -             | Jira API token used for Basic authentication. |
| method                      | String | No       | GET           | HTTP method. Supported values are `GET` and `POST`. |
| headers                     | Map    | No       | -             | Extra HTTP request headers. The connector replaces an `Authorization` entry with the Basic header generated from `email` and `api_token`. |
| params                      | Map    | No       | -             | HTTP query parameters. |
| body                        | String | No       | -             | HTTP request body, usually used with `POST`. |
| format                      | String | No       | TEXT          | Response format. Use `json` when the response should be parsed by `schema`, `json_field`, or `content_field`. |
| schema                      | Config | No       | -             | Output schema. Required when `format = "json"`. |
| json_field                  | Config | No       | -             | JSONPath mapping from response fields to output columns. Must be used together with `schema`. |
| content_field               | String | No       | -             | JSONPath used to select the array or object that should be parsed as rows. |
| pageing                     | Config | No       | -             | Pagination settings. See [Pagination](#pagination). |
| poll_interval_millis        | int    | No       | -             | Poll interval in milliseconds. Jira source is batch-only, so this option is not useful for Jira streaming jobs. |
| retry                       | int    | No       | -             | Maximum retry count when the request throws `IOException`. |
| retry_backoff_multiplier_ms | int    | No       | 100           | Retry backoff multiplier in milliseconds. |
| retry_backoff_max_ms        | int    | No       | 10000         | Maximum retry backoff in milliseconds. |
| json_filed_missed_return_null | boolean | No    | false         | Return `null` when a field configured in `json_field` is missing. |
| common-options              | config | No       | -             | Source common options. See [Source Common Options](../common-options/source-common-options.md). |

### Authentication

Create a Jira API token from your Atlassian account and set:

- `email`: the Jira account email.
- `api_token`: the Jira API token.

The connector builds the Basic authentication header automatically from `email` and `api_token`. If
`headers` also contains `Authorization`, the connector replaces that value with its generated Basic header.

If your Jira site sits behind a proxy, place proxy-specific headers (e.g. `X-Atlassian-Token`)
in `headers` instead of trying to override the Basic auth header.

### Response Parsing

The default `format` is `TEXT`, which returns the whole response as one `content` column.

Use `format = "json"` and `schema` when you want structured output:

```hocon
format = "json"
schema = {
  fields {
    expand = string
    startAt = int
    maxResults = int
    total = string
  }
}
```

Use `content_field` when the rows are inside a nested JSON node. Use `json_field` when columns need to be extracted from different JSONPath expressions.

### Pagination

`pageing` can be used when the target API needs paging parameters.

| name | type | required | default value | description |
|------|------|----------|---------------|-------------|
| total_page_size | long | No | 0 | Total number of pages to request. |
| batch_size | int | No | 100 | Page size returned by each request. |
| start_page_number | long | No | 1 | First page number. |
| page_field | String | No | page | Request parameter name for page number pagination. |
| page_type | String | No | PageNumber | Pagination type. Supported values are `PageNumber` and `Cursor`. |
| cursor_field | String | No | - | Request parameter name for cursor pagination. |
| cursor_response_field | String | No | - | JSONPath field used to read the next cursor from the response. |
| use_placeholder_replacement | boolean | No | false | Use `${field}` placeholder replacement in headers, parameters, and body. |

Jira Cloud enhanced search uses `/rest/api/3/search/jql` and `nextPageToken` cursor pagination.
The removed `/rest/api/3/search` endpoint and its `startAt` pagination must not be used for Cloud.
See the [Atlassian enhanced search reference](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issue-search/).
For a self-hosted Jira deployment, use the endpoint and pagination contract documented for that version.

## Example

### Read Jira Cloud issues with cursor pagination

This GET example passes `jql`, `maxResults`, and subsequent `nextPageToken` values as query parameters.
`keep_page_param_as_http_param = true` lets the reader add the cursor to the request parameters.
The job reads each returned issue's `id` and `key`, and stops when no new cursor is returned.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jira {
    plugin_output = "jira"
    url = "https://example.atlassian.net/rest/api/3/search/jql"
    email = "admin@example.com"
    api_token = "replace-with-token"
    method = "GET"
    params = {
      jql = "project = DEMO AND created >= -30d ORDER BY created ASC"
      maxResults = "100"
    }
    keep_page_param_as_http_param = true
    format = "json"
    content_field = "$.issues.*"
    pageing = {
      page_type = "Cursor"
      cursor_field = "nextPageToken"
      cursor_response_field = "$.nextPageToken"
    }
    schema = {
      fields {
        id = string
        key = string
      }
    }
  }
}

sink {
  Console {
    plugin_input = "jira"
  }
}
```

## Changelog

<ChangeLog />
