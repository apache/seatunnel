# Redis

> Redis sink connector

## 描述

用于将数据写入 Redis。

## 主要功能

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## 选项

|       名称       |   类型   |        是否必须         |  默认值   |
|----------------|--------|---------------------|--------|
| host           | string | 是                   | -      |
| port           | int    | 是                   | -      |
| key            | string | 是                   | -      |
| data_type      | string | 是                   | -      |
| user           | string | 否                   | -      |
| auth           | string | 否                   | -      |
| db_num         | int    | 否                   | 0      |
| mode           | string | 否                   | single |
| nodes          | list   | 当 mode=cluster 时为:是 | -      |
| format         | string | 否                   | json   |
| expire         | long   | 否                   | -1     |
| common-options |        | 否                   | -      |

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

如果将字段名称指定为 `code` 并将 data_type 设置为 `key`，将有两个数据写入 Redis：
1. `200 -> {code: 200, message: true, data: 获取成功}`
2. `500 -> {code: 500, message: false, data: 内部错误}`

如果将字段名称指定为 `value` 并将 data_type 设置为 `key`，则由于上游数据的字段中没有 `value` 字段，将只有一个数据写入 Redis：

1. `value -> {code: 500, message: false, data: 内部错误}`

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

## 更新日志

### 2.2.0-beta 2022-09-26

- 添加 Redis Sink Connector

### 下一个版本

- [改进] 支持 Redis 集群模式连接和用户认证 [3188](https://github.com/apache/seatunnel/pull/3188)

