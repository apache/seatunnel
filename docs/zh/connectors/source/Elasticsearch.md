import ChangeLog from '../changelog/connector-elasticsearch.md';

# Elasticsearch

> Elasticsearch source 连接器

## 简介

支持读取 Elasticsearch2.x 版本和 8.x 版本之间的数据

## Key features

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精准一次](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义的分片](../../introduction/concepts/connector-v2-features.md)

## 配置参数选项

| 参数名称                | 类型    | 是否必须 | 默认值或者描述                             |
| ----------------------- | ------- | -------- |-------------------------------------|
| hosts                   | 数组    | yes      | -                                   |
| auth_type               | string  | no       | basic                               |
| username                | string  | no       | -                                   |
| password                | string  | no       | -                                   |
| auth.api_key_id         | string  | no       | -                                   |
| auth.api_key            | string  | no       | -                                   |
| auth.api_key_encoded    | string  | no       | -                                   |
| index                   | string  | No       | 单索引同步配置，如果index_list没有配置，则必须配置index |
| index_list              | array   | no       | 用来定义多索引同步任务                         |
| source                  | array   | no       | -                                   |
| query                   | json    | no       | {"match_all": {}}                   |
| search_type             | enum    | no       | 查询类型，SQL 或 DSL，默认 DSL              |
| search_api_type         | enum    | no       | 分页 API 类型，SCROLL 或 PIT，默认 SCROLL    |
| sql_query               | json    | no       | SQL 查询语句，当 search_type 为 SQL 时必须    |
| scroll_time             | string  | no       | 1m                                  |
| scroll_size             | int     | no       | 100                                 |
| tls_verify_certificate  | boolean | no       | true                                |
| tls_verify_hostname     | boolean | no       | true                                |
| array_column            | map     | no       |                                     |
| tls_keystore_path       | string  | no       | -                                   |
| tls_keystore_password   | string  | no       | -                                   |
| tls_truststore_path     | string  | no       | -                                   |
| tls_truststore_password | string  | no       | -                                   |
| pit_keep_alive          | long    | no       | 60000 (1 minute)                    |
| pit_batch_size          | int     | no       | 100                                 |
| runtime_fields          | array   | no       | -                                   |
| common-options          |         | no       | -                                   |

### hosts [array]

Elasticsearch 集群的 HTTP 地址，格式为 `host:port`，允许指定多个主机。例如：`["host1:9200", "host2:9200"]`。

## 认证

Elasticsearch 连接器支持多种认证方式，可根据集群的安全配置进行选择。

### auth_type [enum]

指定认证方式，支持：
- `basic`（默认）：使用用户名 + 密码的 HTTP 基本认证
- `api_key`：使用 API Key 的 ID + key 认证
- `api_key_encoded`：使用 Base64 编码后的 API Key 认证

如果未指定，默认使用 `basic` 以兼容旧版本。

### 基本认证

#### username [string]

基本认证的用户名（x-pack 用户名）。

#### password [string]

基本认证的密码（x-pack 密码）。

**示例：**
```hocon
source {
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

#### auth.api_key_id [string]

Elasticsearch 生成的 API Key ID。

#### auth.api_key [string]

Elasticsearch 生成的 API Key 密钥。

#### auth.api_key_encoded [string]

`base64(id:api_key)` 形式的 Base64 编码 API Key，可替代单独提供 ID 与 key。

**注意：** `auth.api_key_id` + `auth.api_key` 与 `auth.api_key_encoded` 只能二选一。

**示例（分开配置 ID 和 key）：**
```hocon
source {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        auth_type = "api_key"
        auth.api_key_id = "your_api_key_id"
        auth.api_key = "your_api_key_secret"
        index = "my_index"
    }
}
```

**示例（使用编码 key）：**
```hocon
source {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        auth_type = "api_key_encoded"
        auth.api_key_encoded = "eW91cl9hcGlfa2V5X2lkOnlvdXJfYXBpX2tleV9zZWNyZXQ="
        index = "my_index"
    }
}
```

### index [string]

Elasticsearch 索引名称，支持 * 模糊匹配。比如存在索引index1,index2,可以指定index*同时读取两个索引的数据。

### source [array]

索引的字段

你可以通过指定字段 `_id` 来获取文档 ID。如果将 `_id` 写入到其他索引，由于 Elasticsearch 的限制，你需要为 `_id` 指定一个别名。

如果你没有配置 `source`，它将自动从索引的映射中获取。

### array_column [map]

由于 Elasticsearch 中没有数组索引，因此需要指定数组类型。

假设tags和phones是数组类型：

```hocon
array_column = {tags = "array<string>",phones = "array<string>"}
```

### query [json]

Elasticsearch 的原生查询语句，用于控制读取哪些数据写入到其他数据源。

### scroll_time [String]

`Seatunnel`底层会使用滚动查询来查询数据，所以需要使用这个参数控制搜索上下文的时间长度。

### scroll_size [int]

滚动查询的最大文档数量。

### index_list [array]

`index_list` 用于定义多索引同步任务。它是一个数组，包含单表同步所需的参数，如 `query`、`source/schema`、`scroll_size` 和 `scroll_time`。建议不要将 `index_list` 和 `query` 配置在同一层级。有关更多详细信息，请参考后面的多表同步示例。

### tls_verify_certificate [boolean]

启用 HTTPS 端点的证书验证

### tls_verify_hostname [boolean]

启用 HTTPS 端点的主机名验证

### tls_keystore_path [string]

PEM 或 JKS 密钥库的路径。该文件必须对运行 SeaTunnel 的操作系统用户可读。

### tls_keystore_password [string]

指定密钥库的密钥密码。

### tls_truststore_path [string]

PEM 或 JKS 信任库的路径。该文件必须对运行 SeaTunnel 的操作系统用户可读。

### tls_truststore_password [string]

指定信任库的密钥密码。

### search_type
查询类型，可选值：
- DSL: 使用 Domain Specific Language 查询（默认）
- SQL: 使用 SQL 查询

### search_api_type
分页 API 类型，可选值：
- SCROLL: 使用 Scroll API 进行分页（默认）
- PIT: 使用 Point in Time (PIT) API 进行分页

### pit_keep_alive [long]
PIT 应保持活动的时间量（以毫秒为单位）

### pit_batch_size  [int]
每次 PIT 搜索请求返回的最大数量

### runtime_fields [array]

在查询时动态计算字段（Elasticsearch 7.11+）。每个 runtime field 需要包含：
- **name**: 字段名
- **type**: 数据类型（boolean, date, double, geo_point, ip, keyword, long）
- **script**: Painless 脚本，用于计算字段值
- **script_lang** (可选): 脚本语言（默认：painless）
- **script_params** (可选): 脚本参数

示例：
```hocon
runtime_fields = [
  {
    name = "day_of_week"
    type = "keyword"
    script = "emit(doc['timestamp'].value.dayOfWeekEnum.toString())"
  },
  {
    name = "total_price"
    type = "double"
    script = "emit(doc['quantity'].value * doc['price'].value)"
  }
]
```

**性能与限制：**
- 运行时字段在查询阶段计算，数据量大时会影响性能
- 适合临时分析、字段试验与低频查询
- 需要 Elasticsearch 7.11 及以上版本

### common options

Source 插件常用参数，具体请参考 [Source 常用选项](../common-options/source-common-options.md)

## 使用案例

案例一

> 案例一会从满足seatunnel-*匹配的索引中按照query读取数据，查询只会返回文档`id`,`name`,`age`,`tags`,`phones` 三个字段。在这个例子中，使用了source字段配置应该读取哪些字段,使用`array_column`指定了`tags`，`phones`应该被当做数组处理。

```hocon
Elasticsearch {
    hosts = ["localhost:9200"]
    index = "seatunnel-*"
    array_column = {tags = "array<string>",phones = "array<string>"}
    source = ["_id","name","age","tags","phones"]
    query = {"range":{"firstPacket":{"gte":1669225429990,"lte":1669225429990}}}
}
```

案例二：多索引同步

> 此示例演示了如何从 `read_index1` 和 `read_index2` 中读取不同的数据，并将其分别写入 `read_index1_copy`,`read_index2_copy` 索引。
> 在 `read_index1` 中，我使用 `source` 来指定要读取的字段，并使用`array_column`指明哪些字段是数组字段。

```hocon
source {
  Elasticsearch {
    hosts = ["https://elasticsearch:9200"]
    username = "elastic"
    password = "elasticsearch"
    tls_verify_certificate = false
    tls_verify_hostname = false
    index_list = [
       {
           index = "read_index1"
           query = {"range": {"c_int": {"gte": 10, "lte": 20}}}
           source = [
           c_map,
           c_array,
           c_string,
           c_boolean,
           c_tinyint,
           c_smallint,
           c_bigint,
           c_float,
           c_double,
           c_decimal,
           c_bytes,
           c_int,
           c_date,
           c_timestamp
           ]
           array_column = {
           c_array = "array<tinyint>"
           }
       }
       {
           index = "read_index2"
           query = {"match_all": {}}
           source = [
           c_int2,
           c_date2,
           c_null
           ]

       }

    ]

  }
}

transform {
}

sink {
  Elasticsearch {
    hosts = ["https://elasticsearch:9200"]
    username = "elastic"
    password = "elasticsearch"
    tls_verify_certificate = false
    tls_verify_hostname = false

    index = "multi_source_write_test_index"
    index_type = "st"
    "schema_save_mode"="CREATE_SCHEMA_WHEN_NOT_EXIST"
    "data_save_mode"="APPEND_DATA"
  }
}
```

案例三：SSL（禁用证书验证）

```hocon
source {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        username = "elastic"
        password = "elasticsearch"

        tls_verify_certificate = false
    }
}
```

案例四：SSL（禁用主机名验证）

```hocon
source {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        username = "elastic"
        password = "elasticsearch"

        tls_verify_hostname = false
    }
}
```

案例五：SSL（启用证书验证）

```hocon
source {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        username = "elastic"
        password = "elasticsearch"

        tls_keystore_path = "${your elasticsearch home}/config/certs/http.p12"
        tls_keystore_password = "${your password}"
    }
}
```

案例六 : sql 方式查询
注意: sql查询不支持map和数组类型
```hocon
source {
  Elasticsearch {
    hosts = ["https://elasticsearch:9200"]
    username = "elastic"
    password = "elasticsearch"
    tls_verify_certificate = false
    tls_verify_hostname = false
    index = "st_index_sql"
    sql_query = "select * from st_index_sql where c_int>=10 and c_int<=20"
    search_type = "sql"
  }
}
```

Demo7:  PIT方式滚动查询
```hocon
source {
  Elasticsearch {
    hosts = ["https://elasticsearch:9200"]
    username = "elastic"
    password = "elasticsearch"
    tls_verify_certificate = false
    tls_verify_hostname = false

    index = "st_index"
    query = {"range": {"c_int": {"gte": 10, "lte": 20}}}

    # 使用 DSL 查询和 PIT API
    search_type = DSL
    search_api_type = PIT
    pit_keep_alive = 60000  # 1 minute in milliseconds
    pit_batch_size = 100
  }
}
```

Demo8: Runtime Fields（Elasticsearch 7.11+）

> 该示例演示如何在查询时计算字段值，而无需重建索引。

```hocon
source {
  Elasticsearch {
    hosts = ["https://elasticsearch:9200"]
    username = "elastic"
    password = "elasticsearch"
    tls_verify_certificate = false
    tls_verify_hostname = false
    
    index = "sales_data"
    
    # 定义运行时字段
    runtime_fields = [
      {
        name = "total_amount"
        type = "double"
        script = "emit(doc['quantity'].value * doc['price'].value)"
      },
      {
        name = "day_of_week"
        type = "keyword"
        script = "emit(doc['order_date'].value.dayOfWeekEnum.getDisplayName(TextStyle.FULL, Locale.ROOT))"
      },
      {
        name = "order_category"
        type = "keyword"
        script = """
          double amount = doc['quantity'].value * doc['price'].value;
          if (amount > 1000) {
            emit('high_value');
          } else if (amount > 100) {
            emit('medium_value');
          } else {
            emit('low_value');
          }
        """
      },
      {
        name = "price_with_tax"
        type = "double"
        script = "emit(doc['price'].value * (1 + params.tax_rate))"
        script_params = {
          tax_rate = 0.13
        }
      }
    ]
    
    source = [
      "product_id",
      "quantity",
      "price",
      "order_date",
      "total_amount",
      "day_of_week",
      "order_category",
      "price_with_tax"
    ]
    
    schema = {
      fields {
        product_id = string
        quantity = int
        price = double
        order_date = timestamp
        total_amount = double
        day_of_week = string
        order_category = string
        price_with_tax = double
      }
    }
  }
}

sink {
  Console {
  }
}
```

## 变更日志

<ChangeLog />
