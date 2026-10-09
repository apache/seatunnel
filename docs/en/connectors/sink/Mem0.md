import ChangeLog from '../changelog/connector-mem0.md';

# Mem0

The Mem0 sink sends SeaTunnel rows to the hosted Mem0 Platform V3 asynchronous
add API. Phase 1 supports `ADD` only. A row is acknowledged only when the API
returns a non-empty `event_id`; this is accepted at-least-once delivery, not
completion of asynchronous processing.

## Sink Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| api_key | String | Yes | - | Mem0 API key, sent as the `Authorization: Token` credential |
| messages_field | String | Yes | - | Input row field containing the messages payload. It must resolve to an array |
| api_base_url | String | No | https://api.mem0.ai | Mem0 Platform V3 API base URL |
| user_id_field | String | No | - | Input row field mapped to Mem0 `user_id` |
| agent_id_field | String | No | - | Input row field mapped to Mem0 `agent_id` |
| app_id_field | String | No | - | Input row field mapped to Mem0 `app_id` |
| run_id_field | String | No | - | Input row field mapped to Mem0 `run_id` |
| metadata_field | String | No | - | Optional input row field mapped to Mem0 `metadata`. It must resolve to a JSON object |
| retry | Int | No | - | The max retry times if the http request throws an `IOException` |
| retry_backoff_multiplier_ms | Int | No | 100 | The retry-backoff times (millis) multiplier if the http request failed |
| retry_backoff_max_ms | Int | No | 10000 | The maximum retry-backoff times (millis) if the http request failed |
| common-options | | No | - | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details |

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

<ChangeLog />
