import ChangeLog from '../changelog/connector-mem0.md';

# Mem0

The Mem0 sink sends SeaTunnel rows to the hosted Mem0 Platform V3 asynchronous
add API. Phase 1 supports `ADD` only. A row is acknowledged only when the API
returns a non-empty `event_id`; this is accepted at-least-once delivery, not
completion of asynchronous processing.

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
