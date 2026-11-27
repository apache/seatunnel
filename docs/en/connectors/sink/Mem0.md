import ChangeLog from '../changelog/connector-mem0.md';

# Mem0

The Mem0 sink sends SeaTunnel rows to the hosted Mem0 Platform V3 asynchronous
add API. Phase 1 supports `ADD` only. A row is acknowledged only when the API
returns a valid JSON object with a nonblank string `event_id`; this is accepted at-least-once delivery, not
completion of asynchronous processing.

Malformed success responses fail the write without being retried by the connector's
HTTP-status retry loop. Diagnostics do not include the response body or JSON parser cause.

## Sink Options

| Name           | Type   | Required | Default               | Description                                                                                                                                                       |
|----------------|--------|----------|-----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| api_key        | String | Yes      | -                     | Mem0 Platform API key. Sent as `Authorization: Token <api_key>`.                                                                                                   |
| api_base_url   | String | No       | `https://api.mem0.ai` | Mem0 Platform V3 API base URL.                                                                                                                                     |
| messages_field | String | Yes      | -                     | Input row field holding the messages payload. It must resolve to an array.                                                                                         |
| user_id_field  | String | No       | -                     | Input row field mapped to Mem0 `user_id`.                                                                                                                          |
| agent_id_field | String | No       | -                     | Input row field mapped to Mem0 `agent_id`.                                                                                                                         |
| app_id_field   | String | No       | -                     | Input row field mapped to Mem0 `app_id`.                                                                                                                           |
| run_id_field   | String | No       | -                     | Input row field mapped to Mem0 `run_id`.                                                                                                                           |
| metadata_field | String | No       | -                     | Input row field mapped to Mem0 `metadata`. It must resolve to a JSON object.                                                                                       |
| retry                        | Integer | No       | -                     | Maximum number of retries when an HTTP request fails with an `IOException`.                                                                                       |
| retry_backoff_multiplier_ms  | Integer | No       | `100`                 | Retry-backoff interval in milliseconds used as the multiplier when a request fails.                                                                               |
| retry_backoff_max_ms         | Integer | No       | `10000`               | Maximum retry-backoff interval in milliseconds when a request fails.                                                                                              |
| common-options |        | No       | -                     | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.                                        |

At least one of `user_id_field`, `agent_id_field`, `app_id_field` and `run_id_field` must be configured and non-empty for every row. The three `retry*` options are inherited from the shared HTTP sink options and apply to request failures.

## Configuration

```hocon
sink {
  Mem0 {
    api_key = "${MEM0_API_KEY}"
    messages_field = "messages"
    user_id_field = "user_id"
    api_base_url = "https://api.mem0.ai"
  }
}
```

`messages_field` must resolve to an array. At least one scope field among
`user_id_field`, `agent_id_field`, `app_id_field`, and `run_id_field` must be
configured and non-empty for each row. Optional `metadata_field` must resolve
to a JSON object.

The connector uses `POST /v3/memories/add/` with `Authorization: Token`,
`Content-Type: application/json`, and `Accept: application/json`. Delete,
self-hosted OSS endpoints, event polling, and a generic Mem0-compatible
protocol are outside Phase 1.

## Sink Options

| Name                        | Type   | Required | Default             | Description                                                              |
|-----------------------------|--------|----------|---------------------|--------------------------------------------------------------------------|
| api_key                     | String | Yes      | -                   | Mem0 Platform API key, sent as the `Authorization: Token` credential.    |
| messages_field              | String | Yes      | -                   | Input row field containing the messages payload. Must resolve to an array. |
| api_base_url                | String | No       | `https://api.mem0.ai` | Mem0 Platform V3 API base URL.                                        |
| user_id_field               | String | No       | -                   | Input row field mapped to Mem0 `user_id`.                                |
| agent_id_field              | String | No       | -                   | Input row field mapped to Mem0 `agent_id`.                               |
| app_id_field                | String | No       | -                   | Input row field mapped to Mem0 `app_id`.                                 |
| run_id_field                | String | No       | -                   | Input row field mapped to Mem0 `run_id`.                                 |
| metadata_field              | String | No       | -                   | Input row field mapped to Mem0 `metadata`. Must resolve to a JSON object. |
| retry                       | int    | No       | -                   | Max retry times when the HTTP request fails with an `IOException`.       |
| retry_backoff_multiplier_ms | int    | No       | 100                 | Retry-backoff multiplier in milliseconds when an HTTP request fails.     |
| retry_backoff_max_ms        | int    | No       | 10000               | Maximum retry-backoff in milliseconds when an HTTP request fails.        |

At least one scope field among `user_id_field`, `agent_id_field`, `app_id_field`,
and `run_id_field` must be configured and non-empty for each row.

<ChangeLog />
