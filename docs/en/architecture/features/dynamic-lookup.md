---
sidebar_position: 4
title: Dynamic Lookup
---

# Dynamic Lookup Architecture

## 1. Overview

Dynamic lookup is an engine-native streaming enrichment action. It joins an append-only fact stream
with a CDC dimension stream in the same SeaTunnel Engine job and emits an enriched output table.

This is an experimental M0 development draft, not a production-certified feature. Runtime,
failure-injection and capacity evidence must be completed before a supported release.

The first supported scope is intentionally narrow:

- the fact side must be append-only
- the dimension side must reject primary-key updates
- schema changes must fail fast
- fact, dimension and lookup parallelism must all be `1`; FORWARD queues cannot co-locate
  independently partitioned keys, and keyed exchange is not implemented yet
- the fact source must have exactly one dynamic lookup consumer
- the dimension bootstrap edge must be dedicated to the lookup
- the runtime uses SeaTunnel Engine checkpoint barriers for recovery

This is not implemented as a normal Transform plugin. The parser creates a dedicated
`DynamicLookupAction` because the runtime must coordinate two source inputs, source gate opening,
checkpoint intent metadata, and dimension state ownership.

## 2. Basic Configuration

Declare dynamic lookup at the top level of the job configuration, next to `source`, `transform`,
and `sink`.

```hocon
env {
  job.mode = "STREAMING"
  parallelism = 1
}

source {
  Kafka {
    plugin_output = "orders_fact"
    topic = "orders"
    # Other Kafka options.
  }

  MySQL-CDC {
    plugin_output = "customer_dimension"
    # Other CDC options.
  }
}

dynamic_lookup {
  orders_with_customer {
    uid = "orders_customer_lookup_v1"
    plugin_output = "orders_enriched"

    fact {
      input = "orders_fact"
      key = ["customer_id"]
      changelog-mode = "APPEND_ONLY"
      required-capability = ["FACT_SOURCE_GATE_V1"]
    }

    dimension {
      input = "customer_dimension"
      table = "inventory.customers"
      key = ["id"]
      primary-key-update = "FAIL"
      required-capability = [
        "ORDERED_BOOTSTRAP_V1",
        "ATOMIC_UPDATE_PAIR_V1",
        "PK_UPDATE_REJECT_V1"
      ]
    }

    join {
      type = "LEFT"
      fields = [
        "fact.order_id",
        "fact.customer_id",
        "fact.amount",
        "dimension.name as customer_name",
        "dimension.level as customer_level"
      ]
    }

    schema-change {
      behavior = "FAIL"
    }

    state {
      backend = "IN_MEMORY"
      ttl = "NONE"
      max-concurrent-snapshots = 1
    }

    resource {
      max-logical-state-bytes-per-subtask = "512mb"
      max-resident-state-bytes-per-subtask = "512mb"
      max-concurrent-snapshots = 1
    }
  }
}

sink {
  Console {
    plugin_input = "orders_enriched"
  }
}
```

## 3. Join Semantics

Dynamic lookup supports two join types:

| `join.type` | Behavior |
|---|---|
| `LEFT` | Emit every fact row. If the dimension key is missing, dimension fields are emitted as null. |
| `INNER` | Emit only fact rows that find a matching dimension row. |

Projection fields must use the `<side>.<field>` syntax. The side must be `fact` or `dimension`.
Aliases use `as`, for example `dimension.name as customer_name`.

The output table schema is built from the selected projection fields. Field types, precision, scale,
and other column metadata are copied from the selected input columns. Fact-side nullability is
copied from the fact column. Dimension-side fields are nullable for `LEFT` joins because the
dimension row can be missing; for `INNER` joins they keep the dimension column nullability.

`INNER` joins drop fact rows that do not find a matching dimension key. The runtime logs the first
miss and powers-of-two miss counts so reconciliation has an audit trail without logging every row.

## 4. Runtime and Recovery Model

At startup, the dimension stream is consumed before the fact stream is opened. The fact source gate
keeps fact splits staged until a checkpoint records the dimension state and the fact positions as a
durable anchor.

The CDC reader reports bootstrap completion only when the incremental split is initialized after
all snapshot splits finish. The source emits an ordered readiness marker before the next barrier.
Checkpoints taken during bootstrap preserve a closed fact gate and may complete normally; they are
not fact-position anchors. Restoring such a checkpoint resumes bootstrap with the gate still closed.
The readiness flag is included in the digest-protected lookup state.

During checkpointing:

1. fact and dimension input barriers are aligned per input port
2. dimension state is snapshotted only after both ports reach the same checkpoint barrier
3. the completed checkpoint stores dynamic lookup intent metadata
4. fact positions become durable from committed checkpoint contents, not from a volatile callback
5. the fact gate is opened after the durable anchor is completed

The `uid` value is the stable checkpoint identity of this dynamic lookup operator. Keep it stable
across restarts and job upgrades; changing it creates a different checkpoint identity and can make
existing lookup state unavailable to the operator.

Restore rejects a lookup parallelism different from its checkpoint before redistributing any state.
Rescaling requires keyed exchange and state redistribution in a later protocol version.

On restore, the dynamic lookup state envelope is verified with a stable payload length and
SHA-256 digest before it is used. Ordinary completed checkpoints keep the legacy raw payload format.
Only dynamic lookup anchor checkpoints use the versioned completed-checkpoint envelope; completed
checkpoints without the envelope use the strict legacy path.

## 5. Source Capability Requirements

The fact source must declare `FACT_SOURCE_GATE_V1`. The first implementation wires this capability
for Kafka. While the gate is closed, Kafka splits are staged and snapshotted through the native
fact gate state envelope instead of being routed back through the enumerator restore path. After the
durable anchor checkpoint completes, the engine sends an open command and staged splits are
activated exactly once.

The dimension source must declare ordered bootstrap and update-pair capabilities. CDC incremental
sources declare:

- `ORDERED_BOOTSTRAP_V1`
- `ATOMIC_UPDATE_PAIR_V1`
- `PK_UPDATE_REJECT_V1`

The dynamic lookup runtime enforces same-key `UPDATE_BEFORE` and `UPDATE_AFTER` pairs. A primary-key
update is treated as a job-failing error.

## 6. M0 Limitations

The first implementation deliberately rejects or limits these cases:

- primary-key updates on the dimension side
- schema change events
- fact changelog modes other than append-only
- fact, dimension or lookup parallelism other than `1`
- restoring lookup state with a different parallelism
- multiple dynamic lookup consumers for the same fact source
- non-dedicated dimension bootstrap edges
- more than one concurrent snapshot per lookup subtask
- logical dimension state larger than 512 MiB per subtask
- disk-backed dimension state and remote staging budgets

If a job needs branch-level gating, remote multi-channel exchange, temporal joins, schema evolution,
dimension primary-key rewrites, or logical state beyond the in-memory M0 limit, it must use a later
protocol version.

The configured 512 MiB limits are admission estimates, not a measured safe heap or throughput
ceiling. Per-entry serialization repeats type descriptors and can reject below the full-map payload
limit; the resident estimate is not a JVM object-layout measurement. Snapshots count bytes first
and serialize directly into one final envelope, avoiding full payload staging copies, but still
perform full-map serialization on the task thread. Capacity certification remains an open gate.
