import ChangeLog from '../changelog/connector-edge-socket.md';

# EdgeSocket

> Edge socket source connector for lightweight remote collectors.

## Support Those Engines

> SeaTunnel Zeta

## Key features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Description

`EdgeSocket` is designed for edge collection scenarios where data is produced by lightweight remote
collectors and sent to a central SeaTunnel Zeta cluster over socket.

This source works as a socket ingress server inside Zeta workers: it binds a host/port and accepts
connections from edge collectors.

Internally, `EdgeSocketSourceReader` keeps a local in-memory queue.
The queue capacity is configured by `local_queue_capacity` (default `1024`).
`pollNext()` only pulls records from this local queue.

When a collector pushes one batch record:

- If queue is full, source replies `RETRY`.
- Otherwise source enqueues the packet and replies `RECEIVED`.

Collector then sends `__COMMIT__:<batchId>` to poll checkpoint confirmation:

- `PENDING`: batch is received but not checkpoint-confirmed.
- `ACK:<watermarkBatchId>`: all batches up to watermark are checkpoint-confirmed.

The queue operation logic is encapsulated in the local queue component (`queue` package),
and the socket ingress logic only decodes packet + delegates enqueue.
Compression handling is executed on queue polling path (`pollNext`) according to packet compression type.

## Data Type Mapping

If no `schema` is configured, this source outputs one string field named `value`.
If `schema` is configured, incoming payload is deserialized as JSON into the defined schema.

## Options

| name                    | type    | required | default value | description |
|-------------------------|---------|----------|---------------|-------------|
| endpoint                | String  | No       | -             | Optional externally reachable ingress endpoint in format `host:port` (for example K8s LB DNS:port or VPC EIP:port). It does not replace `port`; when configured, agent/collector should manually target this endpoint directly instead of automatic discovery. |
| port                    | Integer | Yes      | -             | Ingress bind port on Zeta worker. |
| local_queue_capacity    | Integer | No       | 1024          | Local in-memory queue capacity in source reader, must be greater than 0. |
| max_retries             | Integer | No       | 3             | Global retry budget for ingress socket bind failures. Reader fails after budget is exhausted; `-1` means unlimited. |
| reconnect_interval_ms   | Integer | No       | 1000          | Reopen interval in milliseconds. |
| accept_timeout_ms       | Integer | No       | 1000          | Socket accept/read timeout in milliseconds. |
| packet_mode             | String  | No       | RAW           | Ingress packet mode: `RAW` or `PACKET`. |
| aes_secret_key_base64   | String  | No       | -             | Base64 AES key for decrypting `PACKET` mode payload when `encryption=AES_GCM`. |
| auth_type               | String  | No       | TOKEN         | Authentication type for ingress connection. Current supported value: `TOKEN`. |
| auth_token              | String  | Yes      | -             | Token value used by `TOKEN` auth_type. Collector must send auth line before any data. |
| schema                  | Config  | No       | -             | Optional schema definition. When configured, payload is parsed by JSON deserialization schema. |
| common-options          | -       | No       | -             | Source plugin common parameters. See [Source Common Options](../common-options/source-common-options.md). |

## How to Create an EdgeSocket Synchronization Job

### Minimal configuration (recommended starting point)

```hocon
source {
  EdgeSocket {
    port = 9999
    auth_token = "my-edge-token"
  }
}
```

::::tip Tip
Other options are optional: `auth_type` defaults to `TOKEN`, `packet_mode` defaults to `RAW`, and retry/timeout options use built-in defaults.
::::

### Full configuration example (explicit key options)

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  EdgeSocket {
    port = 9999
    local_queue_capacity = 1024
    packet_mode = "RAW"
    auth_token = "my-edge-token"
  }
}

sink {
  Console {
    parallelism = 1
  }
}
```

If `endpoint` is omitted, source still binds to `0.0.0.0:<port>`, and collector may connect via discovery result (network reachability required).

## How To Choose Access Mode (read first)

### Access Modes (explicit semantics)

- Mode A (manual target, recommended for complex networks): configure `endpoint`; agent/collector connects to this explicit address directly, and automatic discovery is skipped.
- Mode B (automatic discovery, recommended for direct private reachability): do not configure `endpoint`; configure `port` and resolve `workerHost:port` by `jobId`.

When a job contains multiple `EdgeSocket` sources, prefer Mode A so the agent explicitly chooses the target `endpoint`.

Use this 3-step decision:

1. Can collector directly reach the address exposed by Zeta worker?
   - Yes: `endpoint` can be omitted; automatic discovery (`workerHost:port`) is enough.
   - No: set `endpoint` (for example LB / EIP / NAT ingress), and let agent/collector dial it directly.
2. Is your environment K8s / cross-VPC / mixed public-private network (for example EIP)?
   - Yes: prefer a fixed `endpoint` to avoid address drift.
3. Does your edge network have a collector-reachable ingress?
   - No: without a reachable ingress (`endpoint`), direct connection is not possible; with LB/NAT/EIP/gateway ingress, it can still work.

::::tip Tip
`endpoint` is not a local bind address. Source still binds to local listening port (currently `0.0.0.0:<port>`), and `endpoint` is only the address collector should dial.
::::

## Edge Network Access Guide

`EdgeSocket` uses a collector -> source ingress connection model, so the hard requirement is:

- collector can reach the `EdgeSocket` ingress `host:port`;
- source does not dial back to collector.

### Typical Network Scenarios

| Scenario | Can it work directly | Recommendation |
| --- | --- | --- |
| VM in same VPC / routable private network | Yes | `endpoint` can be omitted; use discovery result directly |
| VM across VPCs (routing/firewall opened) | Yes | Prefer `endpoint` for a stable ingress address |
| VM across VPCs (private routing not opened) + VPC EIP | Yes | Use `endpoint=<EIP>:<port>` so collector dials public ingress |
| Public collector -> private worker (no EIP/LB/NAT) | No (by default) | Expose reachable ingress via EIP/LB/NAT, then configure `endpoint` |
| K8s with Service/LB/Ingress | Yes (with ingress entry) | Prefer stable LB/Ingress and set `endpoint` |
| Source-side network is outbound-only (collector direct inbound blocked) | Depends on reachable ingress | With LB/EIP/NAT/gateway ingress it can work; otherwise add relay channel |

### About Outbound-Only Edge Networks

If the network where `EdgeSocket` Source runs is outbound-only, the key question is whether a collector-reachable ingress is available:

- Reachable ingress exists (LB/EIP/NAT/gateway): it can work, and collector dials that ingress (prefer configuring `endpoint`).
- No reachable ingress at all: direct connection is not possible; build relay/tunnel first.

Typical choices:

- deploy a reachable relay/gateway in center; edge collector pushes to relay, relay forwards to `EdgeSocket`;
- use reverse tunnel / private line to convert outbound edge connectivity into a center-side ingress;
- open bidirectional connectivity first (VPN/VPC peering/firewall rules), then use direct ingress.

### K8s Complex-Network Example (recommended)

```hocon
source {
  EdgeSocket {
    # Source still listens inside worker
    port = 10091
    auth_type = "TOKEN"
    auth_token = "edge-token"
    packet_mode = "RAW"
    local_queue_capacity = 2048
    max_retries = 5
    reconnect_interval_ms = 1000
    accept_timeout_ms = 5000

    # Collector direct ingress endpoint (for example LB DNS:port)
    endpoint = "edge-lb.prod.example.com:10091"
  }
}
```

### Simple VM Example (direct private reachability)

```hocon
source {
  EdgeSocket {
    port = 10091
    auth_type = "TOKEN"
    auth_token = "edge-token"
    packet_mode = "RAW"
    local_queue_capacity = 1024
    max_retries = 3
    reconnect_interval_ms = 1000
    accept_timeout_ms = 1000
    # endpoint omitted: collector connects by discovery result
  }
}
```

### VPC + EIP Example (common for public collectors)

```hocon
source {
  EdgeSocket {
    port = 10091
    auth_token = "edge-token"

    # Public EIP of a VM in VPC (or EIP+NAT exposed address)
    endpoint = "203.0.113.10:10091"
  }
}
```

## External Packet Protocol

When `packet_mode = "PACKET"`, each line should be one JSON envelope:

```json
{
  "version": 1,
  "payload": "<base64 payload bytes>",
  "compression": "NONE|GZIP|ZLIB|DEFLATE",
  "encryption": "NONE|AES_GCM",
  "iv": "<base64 iv, required for AES_GCM>"
}
```

Processing order is `decrypt + enqueue` on ingress, then `decompress -> utf-8 string` on queue poll.

## Token Authentication

`auth_type` defaults to `TOKEN`. In this mode, `auth_token` is mandatory.

Collector must send the first line:

```text
__AUTH__:<token>
```

Source replies:

- `ACK`: authentication success, collector can send payload lines
- `AUTH_FAILED`: authentication failed, collector should reconnect with corrected token

## Edge Collector Protocol Reference

For runnable examples, use unit/integration tests under:

- `seatunnel-connectors-v2/connector-edge-socket/src/test/java`
- `seatunnel-e2e/seatunnel-connector-v2-e2e/connector-edge-socket-e2e/src/test/java`

Collector side should follow the snapshot-based batch ACK protocol:

- Send auth line first, wait for `ACK`.
- Send one batch line: `__BATCH__:<batchId>:<payload>`.
- Wait enqueue response: `RECEIVED` or `RETRY`.
- Poll commit with `__COMMIT__:<batchId>`.
- Continue when server replies `ACK:<watermarkBatchId>`.

## Changelog

<ChangeLog />
