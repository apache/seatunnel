# Redis

> Redis sink connector

## 描述

用于将数据写入 Redis。

## 主要功能

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## 选项

| name               | type    |       required        | default value |
|--------------------|---------|-----------------------|---------------|
| host               | string  | `mode=single`时必须      | -             |
| port               | int     | no                    | 6379          |
| key                | string  | yes                   | -             |
| data_type          | string  | yes                   | -             |
| batch_size         | int     | no                    | 10            |
| user               | string  | no                    | -             |
| auth               | string  | no                    | -             |
| db_num             | int     | no                    | 0             |
| mode               | string  | no                    | single        |
| nodes              | list    | yes when mode=cluster | -             |
| format             | string  | no                    | json          |
| expire             | long    | no                    | -1            |
| support_custom_key | boolean | no                    | false         |
| value_field        | string  | no                    | -             |
| hash_key_field     | string  | no                    | -             |
| hash_value_field   | string  | no                    | -             |
| common-options     |         | no                    | -             |

### host [string]

Redis 主机地址

### port [int]

Redis 端口

### key [string]

要写入 Redis 的键值。

例如，如果想使用上游数据中的某个字段值作为键值，可以将该字段名称指定给 key。

上游数据如下：

| code | data | success |
|------|------|---------|
| 200  | 获取成功 | true    |
| 500  | 内部错误 | false   |

如果将字段名称指定为 code 并将 data_type 设置为 key，将有两个数据写入 Redis：
1. `200 -> {code: 200, data: 获取成功, success: true}`
2. `500 -> {code: 500, data: 内部错误, success: false}`
   
如果将字段名称指定为 value 并将 data_type 设置为 key，则由于上游数据的字段中没有 value 字段，将只有一个数据写入 Redis：
1. `value -> {code: 500, data: 内部错误, success: false}`

请参见 data_type 部分以了解具体的写入规则。

当然，这里写入的数据格式只是以 json 为例，具体格式以用户配置的 `format` 为准。

### data_type [string]

Redis 数据类型，支持 `key` `hash` `list` `set` `zset`

- key

> 每个来自上游的数据都会更新到配置的 key，这意味着后面的数据会覆盖前面的数据，只有最后的数据会存储在该 key 中。

- hash

> 每个来自上游的数据会根据字段拆分并写入 hash key，后面的数据会覆盖前面的数据。

- list

> 每个来自上游的数据都会被添加到配置的 list key 中。

- set

> 每个来自上游的数据都会被添加到配置的 set key 中。

- zset

> 每个来自上游的数据都会以权重为 1 的方式添加到配置的 zset key 中。因此，zset 中数据的顺序基于数据的消费顺序。

### user [string]

Redis 认证用户，连接加密集群时需要

### auth [string]

Redis 认证密码，连接加密集群时需要

### db_num [int]

Redis 数据库索引 ID，默认连接到 db 0

### mode [string]

Redis 模式，`single` 或 `cluster`，默认是 `single`

### nodes [list]

Redis 节点信息，在集群模式下使用，必须按如下格式：

["host1:port1", "host2:port2"]

### format [string]

上游数据的格式，目前只支持 `json`，以后会支持 `text`，默认 `json`。

当你指定格式为 `json` 时，例如：

上游数据如下：

| code | data | success |
|------|------|---------|
| 200  | 获取成功 | true    |

连接器会生成如下数据并写入 Redis：

```json
{"code":  200, "data":  "获取成功", "success":  "true"}
```

### expire [long]

设置 Redis 的过期时间，单位为秒。默认值为 -1，表示键不会自动过期。

### support_custom_key [boolean]

设置为true，表示启用自定义Key。

上游数据如下：

| code | data | success |
|------|------|---------|
| 200  | 获取成功 | true    |
| 500  | 内部错误 | false   |

可以使用`{`和`}`符号自定义Redis键名，`{}`中的字段名会被解析替换为上游数据中的某个字段值，例如：将字段名称指定为 `{code}` 并将 data_type 设置为 `key`，将有两个数据写入 Redis：
1. `200 -> {code: 200, data: 获取成功, success: true}`
2. `500 -> {code: 500, data: 内部错误, success: false}`

Redis键名可以由固定部分和变化部分组成，通过Redis分组符号:连接，例如：将字段名称指定为 `code:{code}` 并将 data_type 设置为 `key`，将有两个数据写入 Redis：
1. `code:200 -> {code: 200, data: 获取成功, success: true}`
2. `code:500 -> {code: 500, data: 内部错误, success: false}`

### value_field [string]

要写入Redis的值的字段， `data_type` 支持 `key` `list` `set` `zset`.

当你指定Redis键名字段`key`指定为 `value`，值字段`value_field`指定为`data`，并将`data_type`指定为`key`时,

上游数据如下：

| code | data | success |
|------|------|---------|
| 200  | 获取成功 | true    |

如下的数据会被写入Redis:
1. `value -> 获取成功`

### hash_key_field [string]

要写入Redis的hash键字段, `data_type` 支持 `hash`

### hash_value_field [string]

要写入Redis的hash值字段, `data_type` 支持 `hash`

当你指定Redis键名字段`key`指定为 `value`，hash键字段`hash_key_field`指定为`data`，hash值字段`hash_value_field`指定为`success`，并将`data_type`指定为`hash`时,

上游数据如下：

| code | data | success |
|------|------|---------|
| 200  | 获取成功 | true    |

如下的数据会被写入Redis:
1. `value -> 获取成功 | true`

### common options

Sink 插件通用参数，请参考 [Sink Common Options](../sink-common-options.md) 获取详情

## 示例

简单示例：

```hocon
Redis {
  host = localhost
  port = 6379
  key = age
  data_type = list
}
```

自定义Key示例：

```hocon
Redis {
  host = localhost
  port = 6379
  key = "name:{name}"
  support_custom_key = true
  data_type = key
}
```

自定义Value示例：

```hocon
Redis {
  host = localhost
  port = 6379
  key = person
  value_field = "name"
  data_type = key
}
```

自定义HashKey和HashValue示例：

```hocon
Redis {
  host = localhost
  port = 6379
  key = person
  hash_key_field = "name"
  hash_value_field = "age"
  data_type = hash
}
```

## 更新日志

### 2.2.0-beta 2022-09-26

- 添加 Redis Sink Connector

### 下一个版本

- [改进] 支持 Redis 集群模式连接和用户认证 [3188](https://github.com/apache/seatunnel/pull/3188)

