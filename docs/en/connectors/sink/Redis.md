import ChangeLog from '../changelog/connector-redis.md';

# Redis

> Redis sink connector

## Description

The Redis sink connector writes upstream rows to Redis in batch or streaming jobs. It supports single-node Redis and
Redis Cluster, and can write to `key`/`string`, `hash`, `list`, `set`, and `zset` data types.

The configured `key` can be either a literal Redis key or an upstream field name. When `support_custom_key = true`,
the connector can build the Redis key from one or more upstream fields, for example `user:${id}`.

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Supported DataSource Info

To use the Redis connector, the following dependency is required. It can be installed by `install-plugin.sh` or
downloaded from Maven Central.

| Datasource | Dependency |
|------------|------------|
| Redis      | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-redis) |

## Options

| Name               | Type    | Required                    | Default | Description |
|--------------------|---------|-----------------------------|---------|-------------|
| host               | string  | Yes when `mode = SINGLE`    | -       | Redis host for single-node mode. |
| port               | int     | No                          | 6379    | Redis port for single-node mode. |
| nodes              | list    | Yes when `mode = CLUSTER`   | -       | Redis Cluster nodes, for example `["redis-0:6379", "redis-1:6379"]`. |
| mode               | string  | No                          | SINGLE  | Redis deployment mode. Supported values are `SINGLE` and `CLUSTER`. |
| user               | string  | No                          | -       | Redis ACL username. |
| auth               | string  | No                          | -       | Redis authentication password. |
| db_num             | int     | No                          | 0       | Redis database index. This option is used in single-node mode. |
| key                | string  | Yes                         | -       | Redis key, upstream field name, or key template when `support_custom_key = true`. |
| data_type          | string  | Yes                         | -       | Redis data type. Supported values are `KEY`, `STRING`, `HASH`, `LIST`, `SET`, and `ZSET`. |
| format             | string  | No                          | JSON    | Serialization format used when no value field is configured. Supported values are `JSON` and `TEXT`. |
| field_delimiter    | string  | No                          | `,`     | Field delimiter used when `format = TEXT`. |
| batch_size         | int     | No                          | 10      | Maximum number of rows buffered before one batch write. |
| expire             | long    | No                          | -1      | Key expiration time in seconds. Values less than or equal to 0 mean no expiration is set. |
| support_custom_key | boolean | No                          | false   | Whether to replace placeholders in `key` with upstream field values. |
| value_field        | string  | No                          | -       | Upstream field used as the Redis value for `KEY`/`STRING`, `LIST`, `SET`, and `ZSET`. |
| hash_key_field     | string  | No                          | -       | Upstream field used as the Redis hash field when `data_type = HASH`. |
| hash_value_field   | string  | No                          | -       | Upstream field used as the Redis hash value when `data_type = HASH`. |
| multi_table_sink_replica | int | No                          | 1       | Writer replica count for multi-table writes. |
| common-options     | config  | No                          | -       | Sink plugin common parameters. See [Sink Common Options](../common-options/sink-common-options.md). |

## Write Rules

### key

When `support_custom_key = false`, the connector first checks whether `key` matches an upstream field name:

- If the field exists, the field value is used as the Redis key.
- If the field does not exist, `key` itself is used as a fixed Redis key.

When `support_custom_key = true`, placeholders in `key` are replaced by upstream field values. Both `${field}` and
the legacy `{field}` style are supported.

### data_type

- `KEY` and `STRING`: write one Redis string value for each row. Later rows overwrite earlier rows with the same key.
- `HASH`: write one or more fields into a Redis hash. Configure `hash_key_field` to choose the hash field name.
- `LIST`: append each row value to a Redis list.
- `SET`: add each row value to a Redis set.
- `ZSET`: add each row value to a Redis sorted set with score `1`.

### value_field

For `KEY`/`STRING`, `LIST`, `SET`, and `ZSET`, configure `value_field` when only one upstream field should be written
as the Redis value. If `value_field` is not configured, the connector serializes the whole upstream row using `format`.

### hash_key_field and hash_value_field

For `HASH`, `hash_key_field` chooses the Redis hash field. If `hash_value_field` is configured, that field value is
written as the Redis hash value. If `hash_value_field` is not configured, the connector serializes the whole upstream
row as the hash value.

### multi_table_sink_replica

Replica count for multi-table sink writers. It applies when upstream rows carry table identifiers and the job writes multiple Redis tables in one pipeline.

For multi-table jobs, `key` may include `${table_name}` so rows from different upstream tables are written to separate
Redis keys, for example `key = "redis-result-${table_name}"`.

## Schema Evolution

Redis Sink supports schema evolution with SeaTunnel Zeta. When the upstream is a CDC source, enable
`schema-changes.enabled = true` in the source configuration so schema change events are sent to the sink.

Redis is schema-less, so schema evolution does not execute DDL in Redis. Instead, when Redis Sink serializes the whole
upstream row as JSON or TEXT, it refreshes the serializer after a supported schema change event. Newly added fields are
included, and dropped fields are no longer written. See
[Schema Evolution](../../introduction/configuration/schema-evolution.md) for the supported event types.

Schema evolution does not rewrite field names configured in `key`, custom key placeholders, `value_field`,
`hash_key_field`, or `hash_value_field`. Do not rename or drop a field referenced by these options while the job is
running. If a configured field no longer exists, Redis Sink applies the missing-field behavior described in
[Write Rules](#write-rules), which can turn the configured field name into a literal key or value.

Before applying a schema change, Redis Sink flushes rows buffered with the previous schema. It also stores the latest
schema in checkpoint state and restores that schema after recovery. Restoring a job from a checkpoint taken after a DDL
while increasing the Redis sink parallelism is not currently supported.

## Examples

### Write Rows To A Redis List

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        id = int
        name = string
      }
    }
    rows = [
      { kind = INSERT, fields = [1, "Alice"] },
      { kind = INSERT, fields = [2, "Bob"] }
    ]
  }
}

sink {
  Redis {
    host = "localhost"
    port = 6379
    key = "person_list"
    data_type = LIST
    value_field = "name"
  }
}
```

### Use A Custom Key Template

```hocon
sink {
  Redis {
    host = "localhost"
    port = 6379
    key = "person:${id}"
    support_custom_key = true
    data_type = KEY
    format = JSON
  }
}
```

### Write Hash Fields

```hocon
sink {
  Redis {
    host = "localhost"
    port = 6379
    key = "person_hash"
    data_type = HASH
    hash_key_field = "id"
    hash_value_field = "name"
  }
}
```

### Write To Redis Cluster With Expiration

```hocon
sink {
  Redis {
    mode = CLUSTER
    nodes = ["redis-cluster-0:6379", "redis-cluster-1:6379", "redis-cluster-2:6379"]
    key = "event:${id}"
    support_custom_key = true
    data_type = KEY
    value_field = "name"
    batch_size = 20
    expire = 30
  }
}
```

## Changelog

<ChangeLog />
