import ChangeLog from '../changelog/connector-redis.md';

# Redis

> Redis 接收器连接器

## 描述

Redis 接收器连接器可以在批处理或流处理作业中把上游数据写入 Redis。它支持单节点 Redis 和 Redis Cluster，
可以写入 `key`/`string`、`hash`、`list`、`set`、`zset` 这几类 Redis 数据。

`key` 可以是固定的 Redis key，也可以是上游字段名。开启 `support_custom_key = true` 后，还可以用上游字段
拼出 Redis key，例如 `user:${id}`。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称               | 类型    | 是否必填                    | 默认值 | 描述 |
|--------------------|---------|-----------------------------|--------|------|
| host               | string  | `mode = SINGLE` 时必填      | -      | 单节点模式下的 Redis 主机地址。 |
| port               | int     | 否                          | 6379   | 单节点模式下的 Redis 端口。 |
| nodes              | list    | `mode = CLUSTER` 时必填     | -      | Redis Cluster 节点，例如 `["redis-0:6379", "redis-1:6379"]`。 |
| mode               | string  | 否                          | SINGLE | Redis 部署模式，支持 `SINGLE` 和 `CLUSTER`。 |
| user               | string  | 否                          | -      | Redis ACL 用户名。 |
| auth               | string  | 否                          | -      | Redis 认证密码。 |
| db_num             | int     | 否                          | 0      | Redis 数据库编号，主要用于单节点模式。 |
| key                | string  | 是                          | -      | Redis key、上游字段名，或开启 `support_custom_key` 后的 key 模板。 |
| data_type          | string  | 是                          | -      | Redis 数据类型，支持 `KEY`、`STRING`、`HASH`、`LIST`、`SET`、`ZSET`。 |
| format             | string  | 否                          | JSON   | 未配置 value 字段时的序列化格式，支持 `JSON` 和 `TEXT`。 |
| field_delimiter    | string  | 否                          | `,`    | `format = TEXT` 时使用的字段分隔符。 |
| batch_size         | int     | 否                          | 10     | 每次批量写入前最多缓存的行数。 |
| expire             | long    | 否                          | -1     | Redis key 过期时间，单位秒。小于等于 0 表示不设置过期时间。 |
| support_custom_key | boolean | 否                          | false  | 是否用上游字段值替换 `key` 里的占位符。 |
| value_field        | string  | 否                          | -      | 写入 `KEY`/`STRING`、`LIST`、`SET`、`ZSET` 时，作为 Redis value 的上游字段。 |
| hash_key_field     | string  | 否                          | -      | `data_type = HASH` 时，作为 Redis hash field 的上游字段。 |
| hash_value_field   | string  | 否                          | -      | `data_type = HASH` 时，作为 Redis hash value 的上游字段。 |
| common-options     | config  | 否                          | -      | 接收器插件通用参数，详情请参考[接收器通用选项](../common-options/sink-common-options.md)。 |

## 写入规则

### key

当 `support_custom_key = false` 时，连接器会先判断 `key` 是否是上游字段名：

- 如果字段存在，就使用该字段的值作为 Redis key。
- 如果字段不存在，就把 `key` 本身当作固定 Redis key。

当 `support_custom_key = true` 时，连接器会用上游字段值替换 `key` 里的占位符。`${field}` 和旧格式 `{field}` 都支持。

### data_type

- `KEY` 和 `STRING`：每行写入一个 Redis 字符串。同一个 key 下，后写入的数据会覆盖先写入的数据。
- `HASH`：写入 Redis hash。配置 `hash_key_field` 可以指定 hash field。
- `LIST`：把每行数据追加到 Redis list。
- `SET`：把每行数据加入 Redis set。
- `ZSET`：把每行数据加入 Redis zset，score 固定为 `1`。

### value_field

写入 `KEY`/`STRING`、`LIST`、`SET`、`ZSET` 时，如果只想写入某一个上游字段，可以配置 `value_field`。
如果不配置，连接器会按 `format` 把整行数据序列化后写入 Redis。

### hash_key_field 和 hash_value_field

写入 `HASH` 时，`hash_key_field` 用来指定 Redis hash field。如果配置了 `hash_value_field`，则把这个字段的值
作为 Redis hash value；如果不配置，连接器会把整行数据序列化后作为 hash value。

## 示例

### 写入 Redis List

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

### 使用自定义 Key 模板

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

### 写入 Redis Hash

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

### 写入 Redis Cluster 并设置过期时间

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

## 变更日志

<ChangeLog />
