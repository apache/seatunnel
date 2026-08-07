import ChangeLog from '../changelog/connector-easysearch.md';

# Easysearch

> Easysearch 源连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于从 INFINI Easysearch 读取数据。

## 使用依赖

> 依赖 [easysearch-client](https://central.sonatype.com/artifact/com.infinilabs/easysearch-client)

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列映射](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义拆分](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表读](../../introduction/concepts/connector-v2-features.md)

:::提示

支持的引擎

* 支持 [INFINI Easysearch](https://www.infini.com/download/?product=easysearch) 发布的所有版本。

:::


## 数据类型映射

|    Easysearch 数据类型     | SeaTunnel 数据类型  |
|-----------------------------|----------------------|
| STRING<br/>KEYWORD<br/>TEXT | STRING               |
| BOOLEAN                     | BOOLEAN              |
| BYTE                        | BYTE                 |
| SHORT                       | SHORT                |
| INTEGER                     | INT                  |
| LONG                        | LONG                 |
| FLOAT<br/>HALF_FLOAT        | FLOAT                |
| DOUBLE                      | DOUBLE               |
| DATE                        | LOCAL_DATE_TIME_TYPE |

## Source 选项

| 名称                   | 类型    | 是否必填 | 默认值 | 描述 |
|------------------------|---------|----------|--------|------|
| hosts                  | array   | 是       | -      | Easysearch HTTP 地址。 |
| index                  | string  | 是       | -      | Easysearch 索引名，支持 `seatunnel-*` 这类通配符匹配。 |
| username               | string  | 否       | -      | 开启安全认证时使用的用户名。 |
| password               | string  | 否       | -      | 开启安全认证时使用的密码。 |
| source                 | array   | 否       | -      | 要读取的索引字段。`source` 和 `schema` 二选一配置。 |
| schema                 | config  | 否       | -      | SeaTunnel 读取和转换字段时使用的表结构。`schema` 和 `source` 二选一配置。 |
| query                  | json    | 否       | `{"match_all":{}}` | 用于过滤数据的 Easysearch DSL 查询。 |
| scroll_time            | string  | 否       | 1m     | Easysearch 保留 scroll 查询上下文的时间。 |
| scroll_size            | int     | 否       | 100    | 每次 scroll 请求最多返回的数据条数。 |
| tls_verify_certificate | boolean | 否       | true   | 是否校验 HTTPS 证书。 |
| tls_verify_hostname    | boolean | 否       | true   | 是否校验 HTTPS 主机名。 |
| tls_keystore_path      | string  | 否       | -      | PEM 或 JKS key store 文件路径。 |
| tls_keystore_password  | string  | 否       | -      | key store 密码。 |
| tls_truststore_path    | string  | 否       | -      | PEM 或 JKS trust store 文件路径。 |
| tls_truststore_password | string | 否       | -      | trust store 密码。 |
| common-options         | config  | 否       | -      | Source 插件通用参数。 |

### hosts [array]

Easysearch 集群 HTTP 地址，格式为 `host:port`，允许指定多个主机。例如 `["host1:9200", "host2:9200"]`。

### username [string]

安全用户名。

### password [string]

安全密码。

### index [string]

Easysearch 索引名称，支持 `*` 通配符匹配。

### source [array]

要从索引中读取的字段。

可以通过指定字段 `_id` 来获取文档 id。如果要把 `_id` 写入其他 Easysearch 索引，由于 Easysearch 不允许把
`_id` 当作普通字段写入，需要为 `_id` 指定别名。

`source` 和 `schema` 互斥。如果两者都不配置，连接器会从 Easysearch 读取字段映射，并使用索引中的全部映射字段。

### query [json]

Easysearch DSL 查询，用于控制读取数据的范围。

### scroll_time [String]

Easysearch 为 scroll 请求保留查询上下文的时间。

### scroll_size [int]

每次 Easysearch scroll 请求返回的最大数据条数。

### schema

数据结构，包括字段名和字段类型。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

`schema` 和 `source` 互斥。需要用明确的 SeaTunnel 类型定义转换字段时使用 `schema`。如果两者都不配置，连接器会从
Easysearch 读取字段映射，并使用索引中的全部映射字段。

### tls_verify_certificate [boolean]

为HTTPS端点启用证书验证

### tls_verify_hostname [boolean]

为HTTPS端点启用主机名验证

### tls_keystore_path [string]

PEM或JKS密钥存储的路径。运行SeaTunnel的操作系统用户必须能够读取此文件。

### tls_keystore_password [string]

指定密钥存储的密钥密码

### tls_truststore_path [string]

PEM或JKS信任存储的路径。运行SeaTunnel的操作系统用户必须能够读取此文件.

### tls_truststore_password [string]

指定的信任存储的密钥密码

### 通用选项

Source 插件通用参数，详见 [Source 通用选项](../common-options/source-common-options.md)。

## 示例

### 读取指定字段

```hocon
source {
  Easysearch {
    hosts = ["localhost:9200"]
    index = "seatunnel-*"
    source = ["_id", "name", "age"]
    query = {"range": {"age": {"gte": 18, "lte": 60}}}
  }
}
```

### 使用 Schema 和 Query 读取

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Easysearch {
    hosts = ["https://e2e_easysearch:9200"]
    username = "admin"
    password = "admin"
    tls_verify_certificate = false
    tls_verify_hostname = false

    index = "st_index"
    query = {"range": {"c_int": {"gte": 10, "lte": 20}}}
    schema = {
      fields {
        c_map = "map<string, tinyint>"
        c_array = "array<tinyint>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(2, 1)"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
}

sink {
  Easysearch {
    hosts = ["https://e2e_easysearch:9200"]
    username = "admin"
    password = "admin"
    tls_verify_certificate = false
    tls_verify_hostname = false

    index = "st_index2"
  }
}
```

SSL (禁用证书验证)

```hocon
source {
    Easysearch {
        hosts = ["https://localhost:9200"]
        username = "admin"
        password = "admin"
        
        tls_verify_certificate = false
    }
}
```

SSL (禁用主机名验证)

```hocon
source {
    Easysearch {
        hosts = ["https://localhost:9200"]
        username = "admin"
        password = "admin"
        
        tls_verify_hostname = false
    }
}
```

SSL (启用证书验证)

```hocon
source {
    Easysearch {
        hosts = ["https://localhost:9200"]
        username = "admin"
        password = "admin"
        
        tls_keystore_path = "${your Easysearch home}/config/certs/http.p12"
        tls_keystore_password = "${your password}"
    }
}
```

## 变更日志

<ChangeLog />
