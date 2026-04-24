import ChangeLog from '../changelog/connector-redis.md';

# Redis

> Redis 源连接器

## 描述

用于从 `Redis` 读取数据

## 主要功能

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表读取](../../introduction/concepts/connector-v2-features.md)

## 配置选项

| 名称            | 类型   | 是否必须               | 默认值 | 描述 |
|----------------|--------|--------------------|-------|------|
| host           | string | `mode=single`时必须   | -     | Redis 服务器主机地址 |
| port           | int    | 否                  | 6379  | Redis 服务器端口 |
| user           | string | 否                  | -     | Redis 认证用户名 |
| auth           | string | 否                  | -     | Redis 认证密码 |
| db_num         | int    | 否                  | 0     | Redis 数据库索引 |
| mode           | string | 否                  | single | Redis 模式：`single` 或 `cluster` |
| nodes          | list   | `mode=cluster` 时必须 | -     | Redis 集群节点，格式为 `["host1:port1", "host2:port2"]` |
| tables_configs | list   | 否                  | -     | 多表读取时的表配置列表 |
| common-options |        | 否                  | -     | 源连接器插件通用参数，详情请参见 [Source Common Options](../common-options/source-common-options.md) |

### 表级配置参数

使用 `tables_configs` 读取多个 key 模式时，每个表配置可以包含以下参数：

| 名称                  | 类型     | 是否必须 | 默认值 | 描述                                         |
|---------------------|---------|--------|-------|--------------------------------------------|
| keys                | string  | 是     | -     | 要扫描的 Redis key pattern                     |
| data_type           | string  | 是     | -     | Redis 数据类型：`key`、`hash`、`list`、`set`、`zset` |
| batch_size          | int     | 否     | 10    | SCAN 操作的批量大小                               |
| format              | string  | 否     | json  | 数据格式：`json` 或 `text`                       |
| schema              | config  | 否     | -     | Schema 配置                               |
| hash_key_parse_mode | string  | 否     | all   | Hash key 解析模式：`all` 或 `kv`                 |
| read_key_enabled    | boolean | 否     | false | 是否在输出中包含 Redis key                         |
| key_field_name      | string  | 否     | -     | Redis key 的字段名称                            |
| single_field_name   | string  | 否     | -     | 单值类型的字段名称                                  |
| field_delimiter     | string  | 否     | ','   | 文本格式的分隔符                                   |

**注意：** 当配置对应单个表时，可以将 tables_configs 中的配置项平铺到外层（向后兼容）。

**重要提示：** 在多表模式下，上述表级参数需要配置在 `tables_configs` 的每个表项中。

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

当指定格式为 `text` 时，可以选择是否指定schema参数。

例如, 当上游数据如下时：

```text
200#get success#true
```

如果不指定schema参数，连接器将按照以下方式处理上游数据：

| content                                                  |
| -------------------------------------------------------- |
| 200#get success#true |

如果指定schema参数，此时需要同时配置`schema`和`field_delimiter`，如下所示：
```hocon
field_delimiter = "#"
schema {
    fields {
        code = int
        data = string
        success = boolean
    }
}

```

连接器将生成如下数据：

| content                                                  |
| -------------------------------------------------------- |
| {"code":  200, "data":  "get success", "success":  true} |

### field_delimiter [string]
字段分隔符，用于告诉连接器如何分割字段。

目前仅当格式为text时需要配置。默认为","。

### schema [config]

#### fields [config]

Redis 数据的 schema 字段列表。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

### common options

源连接器插件通用参数，详情请参见 [Source Common Options](../common-options/source-common-options.md)

## 示例

### 单表模式
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

### 多表模式

**示例 1：读取具有不同数据类型的多个 key pattern**

```hocon
env {
  job.mode = "BATCH"
}

source {
  Redis {
    host = "localhost"
    port = 6379
    auth = "password"
    db_num = 0
    tables_configs = [
      {
        keys = "user:active:*"
        data_type = STRING
        format = JSON
        batch_size = 50
        schema {
          fields {
            id = int
            name = string
            email = string
            created_at = timestamp
          }
        }
      },
      {
        keys = "session:*"
        data_type = HASH
        hash_key_parse_mode = KV
        read_key_enabled = true
        key_field_name = "session_id"
        schema {
          fields {
            session_id = string
            user_id = int
            ip_address = string
            last_active = timestamp
          }
        }
      },
      {
        keys = "queue:task:*"
        data_type = LIST
        format = TEXT
        field_delimiter = "|"
      }
    ]
  }
}

sink {
  Console {
    parallelism = 1
  }
}
```

**示例 2：集群模式下的多表配置**

```hocon
source {
  Redis {
    mode = CLUSTER
    nodes = ["node1:6379", "node2:6379", "node3:6379"]
    auth = "cluster_password"
    tables_configs = [
      {
        keys = "metric:cpu:*"
        data_type = STRING
        format = JSON
        batch_size = 10
        schema {
          fields {
            host = string
            timestamp = timestamp
            usage = double
          }
        }
      },
      {
        keys = "metric:memory:*"
        data_type = STRING
        format = JSON
        batch_size = 10
        schema {
          fields {
            host = string
            timestamp = timestamp
            used = long
            total = long
          }
        }
      }
    ]
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />