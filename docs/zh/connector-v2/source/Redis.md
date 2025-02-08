# Redis

> Redis 源连接器

## 描述

用于从 `Redis` 读取数据

## 主要功能

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [ ] [列投影](../../concept/connector-v2-features.md)
- [ ] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../concept/connector-v2-features.md)

## 配置选项

| 名称                  | 类型     | 是否必须               | 默认值    |
|---------------------|--------|--------------------|--------|
| host                | string | 是                  | -      |
| port                | int    | 是                  | -      |
| keys                | string | 是                  | -      |
| batch_size          | int    | 是                  | 10     |
| data_type           | string | 是                  | -      |
| user                | string | 否                  | -      |
| auth                | string | 否                  | -      |
| db_num              | int    | 否                  | 0      |
| mode                | string | 否                  | single |
| hash_key_parse_mode | string | 否                  | all    |
| nodes               | list   | `mode=cluster` 时必须 | -      |
| schema              | config | `format=json` 时必须  | -      |
| format              | string | 否                  | json   |
| common-options      |        | 否                  | -      |

### host [string]

redis 主机地址

### port [int]

redis 端口号

### hash_key_parse_mode [string]

指定 hash key 解析模式, 支持 `all` `kv` 模式, 用于设定连接器如何解析 hash key。

当设定为 `all` 时，连接器会将 hash key 的值视为一行并根据 schema config 配置进行解析，当设定为 `kv` 时，连接器会将 hash key 的每个 kv 视为一行，并根据 schema config 进行解析。

例如，如果 hash key 的值如下设置：

```text
{ 
  "001": {
    "name": "tyrantlucifer",
    "age": 26
  },
  "002": {
    "name": "Zongwen",
    "age": 26
  }
}

```

如果 `hash_key_parse_mode` 设置为 `all` 模式，且 schema config 如下所示，将会生成下表数据：

```hocon
schema {
  fields {
    001 {
      name = string
      age = int
    }
    002 {
      name = string
      age = int
    }
  }
}

```

| 001                             | 002                       |
| ------------------------------- | ------------------------- |
| Row(name=tyrantlucifer, age=26) | Row(name=Zongwen, age=26) |

如果 `hash_key_parse_mode` 设置为 `kv` 模式，且 schema config 如下所示，将会生成下表数据：

```hocon
schema {
  fields {
    hash_key = string
    name = string
    age = int
  }
}

```

| hash_key | name          | age  |
| -------- | ------------- | ---- |
| 001      | tyrantlucifer | 26   |
| 002      | Zongwen       | 26   |

hash key 中的每个 kv 将会被视为一行并被发送给上游。

**提示：连接器将使用 scheme config 的第一个字段信息作为每个 kv 中每个 k 的字段名称**

### keys [string]

keys 模式

### batch_size [int]

表示每次迭代尝试返回的键的数量，默认值为 10。

**提示：Redis 连接器支持模糊键匹配，用户需要确保匹配的键类型相同**

### data_type [string]

redis 数据类型, 支持 `key` `hash` `list` `set` `zset`。

- key

> 将每个 key 的值将作为单行数据发送给下游。  
> 例如，key 对应的值为 `SeaTunnel test message`，则下游接收到的数据为 `SeaTunnel test message`，并且仅会收到一条信息。

- hash

> hash 键值对将会被格式化为 json，并以单行数据的形式发送给下游。  
> 例如，hash 值为 `name:tyrantlucifer age:26`，则下游接收到的数据为 `{"name":"tyrantlucifer", "age":"26"}`，并且仅会收到一条信息。

- list

> list 中的每个元素都将作为单行数据向下游发送。  
> 例如，list 值为 `[tyrantlucier, CalvinKirs]`，则下游接收到的数据为 `tyrantlucifer` 和 `CalvinKirs`，并且仅会收到两条信息。

- set

> set 中的每个元素都将作为单行数据向下游发送。  
> 例如，set 值为 `[tyrantlucier, CalvinKirs]`，则下游接收到的数据为 `tyrantlucifer` 和 `CalvinKirs`，并且仅会收到两条信息。

- zset

> zset 中的每个元素都将作为单行数据向下游发送。  
> 例如，zset 值为 `[tyrantlucier, CalvinKirs]`，则下游接收到的数据为 `tyrantlucifer` 和 `CalvinKirs`，并且仅会收到两条信息。

### user [string]

Redis 认证身份用户，当连接到加密集群时需要使用

### auth [string]

Redis 认证密钥，当连接到加密集群时需要使用

### db_num [int]

Redis 数据库索引 ID，默认将连接到 db 0

### mode [string]

Redis 模式，`single` 或 `cluster`，默认值为 `single`

### nodes [list]

Redis 节点信息，在 cluster 模式下使用，必须设置为以下格式：

["host1:port1", "host2:port2"]

### format [string]

上游数据格式，目前仅支持 `json` `text`，默认为 `json`

当指定格式为 `json` 时，还需要指定 scheme option，例如：

当上游数据如下时：

```json
{"code":  200, "data":  "get success", "success":  true}

```

需要指定 schema 为如下配置：

```hocon
schema {
    fields {
        code = int
        data = string
        success = boolean
    }
}

```

连接器将会生成如下格式数据：

| code | data        | success |
| ---- | ----------- | ------- |
| 200  | get success | true    |

当指定格式为 `text` 时，连接器不会对上游数据做任何处理，例如：

当上游数据如下时：

```json
{"code":  200, "data":  "get success", "success":  true}

```

连接器将会生成如下格式数据：

| content                                                  |
| -------------------------------------------------------- |
| {"code":  200, "data":  "get success", "success":  true} |

### schema [config]

#### fields [config]

Redis 数据的 schema 字段列表

### common options

源连接器插件通用参数，详情请参见 [Source Common Options](../source-common-options.md)

## 示例

简单使用示例：

```hocon
Redis {
  host = localhost
  port = 6379
  keys = "key_test*"
  data_type = key
  format = text
}
```

```hocon
Redis {
  host = localhost
  port = 6379
  keys = "key_test*"
  data_type = key
  format = json
  schema {
    fields {
      name = string
      age = int
    }
  }
}
```

读取 string 类型并附加到 list 示例：


```hocon
source {
  Redis {
    host = "redis-e2e"
    port = 6379
    auth = "U2VhVHVubmVs"
    keys = "string_test*"
    data_type = string
    batch_size = 33
  }
}

sink {
  Redis {
    host = "redis-e2e"
    port = 6379
    auth = "U2VhVHVubmVs"
    key = "string_test_list"
    data_type = list
    batch_size = 33
  }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Redis Source Connector

### next version

- [Improve] Support redis cluster mode connection and user authentication [3188](https://github.com/apache/seatunnel/pull/3188)
-  [Bug] Redis scan command supports versions 5, 6, 7 [7666](https://github.com/apache/seatunnel/pull/7666)