import ChangeLog from '../changelog/connector-http-posthog.md';

# PostHog

> PostHog source connector

## Description

Reads the result of one HogQL query from PostHog. The connector uses the synchronous Query API and runs as a bounded batch source.

For large historical exports, use [PostHog Batch Exports](https://posthog.com/docs/cdp/batch-exports) instead of issuing one large HogQL query.

## Key Features

- [x] [Batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [Stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [Exactly-Once](../../introduction/concepts/connector-v2-features.md)
- [x] [Column Projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [Parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [Support User-Defined Split](../../introduction/concepts/connector-v2-features.md)

## Source Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| base_url | String | No | https://us.posthog.com | PostHog instance base URL. Use `https://eu.posthog.com` for PostHog EU Cloud, or the URL of a self-hosted instance. |
| project_id | String | Yes | - | PostHog project ID. |
| api_key | String | Yes | - | PostHog personal API key with `query:read` permission. |
| query | String | Yes | - | HogQL query executed through the PostHog Query API. |
| schema | Config | Yes | - | Output schema. Every schema field must match a returned HogQL column name or alias. |
| headers | Map | No | - | Additional HTTP headers. The connector sets the authorization and JSON headers. |
| retry | int | No | 0 | Maximum number of HTTP request attempts after I/O failures. |
| retry_backoff_multiplier_ms | int | No | 100 | Retry backoff multiplier in milliseconds. |
| retry_backoff_max_ms | int | No | 10000 | Maximum retry backoff in milliseconds. |
| connect_timeout_ms | int | No | 12000 | HTTP connection timeout in milliseconds. |
| socket_timeout_ms | int | No | 60000 | HTTP socket timeout in milliseconds. |
| common-options | Config | No | - | Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md). |

## Usage Notes

- The connector executes the configured query once and then finishes. Add a `LIMIT` and appropriate time filters so that the result fits in one PostHog Query API response.
- HogQL expressions may return generated column names. Use `AS` aliases so every selected column matches a field in `schema`.
- Keep `api_key` outside shared configuration files by using SeaTunnel variable substitution or the secret mechanism of the deployment platform.
- The connector does not use PostHog's deprecated events-list endpoint and does not rewrite the supplied HogQL query.

## Task Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  PostHog {
    base_url = "https://us.posthog.com"
    project_id = "12345"
    api_key = "${POSTHOG_API_KEY}"
    query = "SELECT event, distinct_id, timestamp FROM events WHERE timestamp >= now() - INTERVAL 1 DAY ORDER BY timestamp LIMIT 10000"
    schema = {
      fields {
        event = string
        distinct_id = string
        timestamp = timestamp
      }
    }
  }
}

sink {
  Console {
  }
}
```

<ChangeLog />
