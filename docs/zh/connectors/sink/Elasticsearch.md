import ChangeLog from '../changelog/connector-elasticsearch.md';

# Elasticsearch

## 描述

输出数据到 `Elasticsearch`

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)

:::tip

引擎支持

* 支持  `ElasticSearch 版本 >= 2.x 并且 <= 8.x`

:::
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## 选项

|           名称           | 类型      | 是否必须 |             默认值              |
|------------------------|---------|------|------------------------------|
| hosts                  | array   | 是    | -                            |
| index                  | string  | 是    | -                            |
| schema_save_mode       | string  | 是    | CREATE_SCHEMA_WHEN_NOT_EXIST |
| data_save_mode         | string  | 是    | APPEND_DATA                  |
| index_type             | string  | 否    |                              |
| primary_keys           | list    | 否    |                              |
| key_delimiter          | string  | 否    | `_`                          |
| auth_type              | string  | 否    | basic                        |
| username               | string  | 否    |                              |
| password               | string  | 否    |                              |
| auth.api_key_id        | string  | 否    | -                            |
| auth.api_key           | string  | 否    | -                            |
| auth.api_key_encoded   | string  | 否    | -                            |
| max_retry_count        | int     | 否    | 3                            |
| max_batch_size         | int     | 否    | 10                           |
| tls_verify_certificate | boolean | 否    | true                         |
| tls_verify_hostname    | boolean | 否    | true                         |
| tls_keystore_path      | string  | 否    | -                            |
| tls_keystore_password  | string  | 否    | -                            |
| tls_truststore_path    | string  | 否    | -                            |
| tls_truststore_password | string  | 否    | -                            |
| common-options         |         | 否    | -                            |
| vectorization_fields   | array   | 否    | -                            |
| vector_dimensions      | int     | 否    | -                            |

### hosts [array]

`Elasticsearch` 集群http地址，格式为 `host:port` ，允许指定多个主机。例如 `["host1:9200"， "host2:9200"]`

### index [string]

`Elasticsearch` 的 `index` 名称。索引支持包含字段名变量，例如 `seatunnel_${age}`(需要配置schema_save_mode="IGNORE")，并且该字段必须出现在 seatunnel Row 中。如果没有，我们将把它视为普通索引

### index_type [string]

`Elasticsearch` 索引类型，elasticsearch 6及以上版本建议不要指定

### primary_keys [list]

主键字段用于生成文档 `_id` ，这是 CDC 必需的选项。

### key_delimiter [string]

设定复合键的分隔符（默认为 `_`），例如，如果使用 `$` 作为分隔符，那么文档的 `_id` 将呈现为 `KEY1$KEY2$KEY3` 的格式

## 认证

Elasticsearch 连接器支持多种认证方式连接到安全的 Elasticsearch 集群。您可以根据 Elasticsearch 的安全配置选择合适的认证方式。

### auth_type [enum]

指定使用的认证方式。支持的值：
- `basic`（默认）：使用用户名和密码的 HTTP 基本认证
- `api_key`：使用独立的 ID 和密钥的 Elasticsearch API Key 认证
- `api_key_encoded`：使用编码密钥的 Elasticsearch API Key 认证

如果未指定，默认使用 `basic` 以保持向后兼容。

### 基本认证

基本认证使用 HTTP 基本认证，通过用户名和密码凭据进行认证。

#### username [string]

基本认证用户名（x-pack 用户名）。

#### password [string]

基本认证密码（x-pack 密码）。

**示例：**
```hocon
sink {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        auth_type = "basic"
        username = "elastic"
        password = "your_password"
        index = "my_index"
    }
}
```

### API Key 认证

API Key 认证提供了一种更安全的方式，使用 API 密钥对 Elasticsearch 进行认证。

#### auth.api_key_id [string]

Elasticsearch 生成的 API 密钥 ID。

#### auth.api_key [string]

Elasticsearch 生成的 API 密钥。

#### auth.api_key_encoded [string]

Base64 编码的 API 密钥，格式为 `base64(id:api_key)`。这是分别指定 `auth.api_key_id` 和 `auth.api_key` 的替代方式。

**注意：** 可以使用 `auth.api_key_id` + `auth.api_key` 或 `auth.api_key_encoded`，但不能同时使用两者。

**使用独立 ID 和密钥的示例：**
```hocon
sink {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        auth_type = "api_key"
        auth.api_key_id = "your_api_key_id"
        auth.api_key = "your_api_key_secret"
        index = "my_index"
    }
}
```

**使用编码密钥的示例：**
```hocon
sink {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        auth_type = "api_key_encoded"
        auth.api_key_encoded = "eW91cl9hcGlfa2V5X2lkOnlvdXJfYXBpX2tleV9zZWNyZXQ="
        index = "my_index"
    }
}
```

### max_retry_count [int]

批次批量请求最大尝试大小

### vectorization_fields [array]
需要向量转换的字段名，Elasticsearch 7.3及以后的版本支持

### vector_dimensions [int]
向量维度，Elasticsearch 7.3及以后的版本支持

### max_batch_size [int]

批次批量文档最大大小

### tls_verify_certificate [boolean]

为 HTTPS 端点启用证书验证

### tls_verify_hostname [boolean]

为 HTTPS 端点启用主机名验证

### tls_keystore_path [string]

指向 PEM 或 JKS 密钥存储的路径。运行 SeaTunnel 的操作系统用户必须能够读取此文件

### tls_keystore_password [string]

指定的密钥存储的密钥密码

### tls_truststore_path [string]

指向 PEM 或 JKS 信任存储的路径。运行 SeaTunnel 的操作系统用户必须能够读取此文件

### tls_truststore_password [string]

指定的信任存储的密钥密码

### common options

Sink插件常用参数，请参考 [Sink常用选项](../common-options/sink-common-options.md) 了解详情

### schema_save_mode

在启动同步任务之前，针对目标侧已有的表结构选择不同的处理方案<br/>
选项介绍：<br/>
`RECREATE_SCHEMA` ：当表不存在时会创建，当表已存在时会删除并重建<br/>
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：当表不存在时会创建，当表已存在时则跳过创建<br/>
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：当表不存在时将抛出错误<br/>
`IGNORE` ：忽略对表的处理<br/>

### data_save_mode

在启动同步任务之前，针对目标侧已存在的数据选择不同的处理方案<br/>
选项介绍：<br/>
`DROP_DATA`： 保留数据库结构，删除数据<br/>
`APPEND_DATA`：保留数据库结构，保留数据<br/>
`ERROR_WHEN_DATA_EXISTS`：当有数据时抛出错误<br/>

## 示例

简单示例

```conf
sink {
    Elasticsearch {
        hosts = ["localhost:9200"]
        index = "seatunnel-${age}"
        schema_save_mode="IGNORE"
    }
}
```

多表写入

```conf
sink {
    Elasticsearch {
        hosts = ["localhost:9200"]
        index = "${table_name}"
        schema_save_mode="IGNORE"
    }
}
```
向量转换(vector data)

```conf
sink {
    Elasticsearch {
        hosts = ["localhost:9200"]
        index = "${table_name}"
        schema_save_mode="IGNORE"
        vectorization_fields = ["review_embedding"]  
        vector_dimensions = 1024 
    }
}
```

变更数据捕获 (Change data capture) 事件

```conf
sink {
    Elasticsearch {
        hosts = ["localhost:9200"]
        index = "seatunnel-${age}"
        schema_save_mode="IGNORE"
        # CDC required options
        primary_keys = ["key1", "key2", ...]
    }
}
```

```
变更数据捕获 (Change data capture) 事件多表写入

```conf
sink {
    Elasticsearch {
        hosts = ["localhost:9200"]
        index = "${table_name}"
        schema_save_mode="IGNORE"
        primary_keys = ["${primary_key}"]
    }
}
```

SSL 禁用证书验证

```hocon
sink {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        username = "elastic"
        password = "elasticsearch"
        
        tls_verify_certificate = false
    }
}
```

SSL 禁用主机名验证

```hocon
sink {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        username = "elastic"
        password = "elasticsearch"
        
        tls_verify_hostname = false
    }
}
```

SSL 启用证书验证

通过设置 `tls_keystore_path` 与 `tls_keystore_password` 指定证书路径及密码

```hocon
sink {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        username = "elastic"
        password = "elasticsearch"
        
        tls_keystore_path = "${your elasticsearch home}/config/certs/http.p12"
        tls_keystore_password = "${your password}"
    }
}
```

配置表生成策略

通过设置 `schema_save_mode` 配置为 `CREATE_SCHEMA_WHEN_NOT_EXIST` 来支持不存在表时创建表

```hocon
sink {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        username = "elastic"
        password = "elasticsearch"
        
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode = "APPEND_DATA"
    }
}
```

## 模式演变

CDC采集支持有限数量的模式更改。目前支持的模式更改包括：

* 添加列。

### 模式演变
```hocon
env {
  # You can set engine configuration here
  parallelism = 5
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  read_limit.bytes_per_second=7000000
  read_limit.rows_per_second=400
}

source {
  MySQL-CDC {
    server-id = 5652-5657
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    schema-changes.enabled = true
  }
}

sink {
  Elasticsearch {
    hosts = ["https://elasticsearch:9200"]
    username = "elastic"
    password = "elasticsearch"
    tls_verify_certificate = false
    tls_verify_hostname = false
    index = "schema_change_index"
    index_type = "_doc"
    "schema_save_mode"="CREATE_SCHEMA_WHEN_NOT_EXIST"
    "data_save_mode"="APPEND_DATA"
  }
}
```

## 变更日志

<ChangeLog />