---
sidebar_position: 7
title: Output Configuration
---

# Output Configuration

The output section defines where the agent sends batched records after they are written to the local outbound queue. Production setups use type: transport (EdgeSocket TCP). Local debugging can use type: console.

For every key, type, and default, see [Configuration Reference — output](configuration.md#output). This page covers alignment with the engine, RAW vs PACKET, and scenario YAML.

## Transport vs console

| output.type | Use case |
|---------------|----------|
| transport | Send to a running job via EdgeSocket (endpoint, token, …). |
| console | Log serialized payloads as EDGE_CONSOLE_OUTPUT in log/edge-agent.log (not business stdout). Default when type is omitted. |

## Engine endpoint vs Agent output.endpoint

:::caution endpoint confusion

Do not confuse the EdgeSocket Source endpoint in the engine job with the agent output.endpoint. They serve different roles.

:::

| Configuration | Required | Semantics |
|---------------|----------|-----------|
| EdgeSocket Source port | Yes | Engine listener port (typically on all interfaces). |
| EdgeSocket Source endpoint | No | Observability / logging only; does not change bind address. |
| Agent output.endpoint | Yes | TCP dial target from the edge host: reachable host:port. |

Anti-pattern: setting engine Source endpoint to 0.0.0.0:9876 and using the same value in output.endpoint. The agent must use a routable address.

## Endpoint and authentication

| Concern | Agent (agent.yaml) | Engine (EdgeSocket Source job) |
|---------|----------------------|--------------------------------|
| Dial / listen | `output.endpoint: "<host>:<port>"` — must be reachable from the edge host | port — listener port for the source |
| Shared secret | output.token | token (or equivalent in job config) |

The agent sends `__AUTH__:<token>` first. Typical responses:

| Response | Meaning | Agent behavior |
|----------|---------|----------------|
| ACK | Auth OK | Proceed to send batches |
| REJECTED | Duplicate collector or policy conflict | Fail fast; do not auto-reconnect — check for a second agent on the same listener |
| AUTH_FAILED | Token mismatch | Fix YAML vs job config and restart |

Wire details: [EdgeSocket Source](../connectors/source/EdgeSocket.md).

## RAW vs PACKET

| Mode | Default | Behavior |
|------|---------|----------|
| RAW | Yes | Payload bytes after event serialization are sent as one batch body. Simplest path for quick start and most log pipelines. |
| PACKET | No | Framed packets with compression and encryption support. Must match EdgeSocket source packet settings on the engine. |

:::caution Compression and encryption

compression and encryption apply only when packet-mode: PACKET. With RAW, values in YAML are ignored. The configuration table default for compression is gzip for PACKET mode only.

:::

### Batch responses

After auth, each batch uses `__BATCH__:<batchId>:<payload>`. Common engine replies:

| Response | Agent handling |
|----------|----------------|
| RECEIVED | Outbound queue row marked acknowledged |
| RETRY | Resend same batch |
| `QUEUE_FULL:<ms>` | Wait and retry |
| DECRYPT_FAILED | Fatal — fix PACKET/encryption alignment with the engine |

:::note

The agent does not send `__COMMIT__`. Durability relies on the local SQLite outbound queue until RECEIVED.

:::

## Scenario examples

Each snippet is output only; use it with input and queue / retry settings for your deployment.

### 1. Production — RAW + token

```yaml
output:
  type: transport
  endpoint: "10.0.1.50:9876"
  auth-type: token
  token: "<same-as-engine-token>"
  packet-mode: RAW
  connect-timeout-ms: 5000
  read-timeout-ms: 30000
```

### 2. Local debug — console

```yaml
output:
  type: console
```

:::tip

Start the agent and search log/edge-agent.log for EDGE_CONSOLE_OUTPUT — no Engine job required. See [Quick Start — Console local test](quick-start.md#console-local-test).

:::

### 3. PACKET + gzip

```yaml
output:
  type: transport
  endpoint: "10.0.1.50:9876"
  auth-type: token
  token: "<shared-secret>"
  packet-mode: PACKET
  compression: gzip
  encryption: none
```

Align compression and framing with the EdgeSocket source job configuration.

### 4. PACKET + AES-GCM

```yaml
output:
  type: transport
  endpoint: "10.0.1.50:9876"
  auth-type: token
  token: "<shared-secret>"
  packet-mode: PACKET
  compression: none
  encryption: aes_gcm
  aes-secret-key-base64: "<base64-key-matching-engine>"
```

:::caution

aes-secret-key-base64 is required when encryption is aes_gcm, and must match the engine side.

:::

### 5. Unstable network — longer timeouts and reconnect

```yaml
output:
  type: transport
  endpoint: "10.0.1.50:9876"
  auth-type: token
  token: "<shared-secret>"
  packet-mode: RAW
  connect-timeout-ms: 10000
  read-timeout-ms: 60000
  initial-backoff-ms: 500
  max-backoff-ms: 60000
  max-reconnect-cycles: 32
  max-batch-send-attempts: 128
```

:::note

Transport reconnect is separate from WAL retry.* (scheduler / outbound row replay).

:::

## output.id and migration

output.id labels the outbound side for logs and migration only (not on the wire). See [Identity file](configuration.md#identity-file-edge-agentid). When moving hosts, copy edge-agent.id with the WAL database and config/agent.yaml.

