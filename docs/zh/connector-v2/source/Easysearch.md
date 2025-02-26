# Easysearch

> Easysearch 源连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于从INFINI Easysearch读取数据。

## 使用依赖

> 依赖 [easysearch-client](https://central.sonatype.com/artifact/com.infinilabs/easysearch-client)

## 关键特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [列映射](../../concept/connector-v2-features.md)
- [ ] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义拆分](../../concept/connector-v2-features.md)

:::提示

支持的引擎

* 支持发布的所有版本 [INFINI Easysearch](https://www.infini.com/download/?product=easysearch).


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
| Date                        | LOCAL_DATE_TIME_TYPE |

### hosts [array]

Easysearch集群http地址，格式为“host:port”，允许指定多个主机。例如`[“host1:9200”，“host2:9200”]`。

### username [string]

安全用户名。

### password [string]

安全密码。

### index [string]

Easysearch搜索索引名称，支持*模糊匹配。

### source [array]

索引字段。
您可以通过指定字段“_id”来获取文档id。如果sink_id指向其他索引，由于Easysearch的限制，您需要为_id指定一个别名。
若不配置源代码，则必须配置`schema`。

### query [json]

Easysearch DSL.
您可以控制读取数据的范围。

### scroll_time [String]

Easysearch将为滚动请求保持搜索上下文活动的时间量。

### scroll_size [int]

每次Easysearch滚动请求返回的最大请求数。

### schema

数据的结构，包括字段名和字段类型。
如果不配置schema，则必须配置`source`。

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

### common options

Source插件常用参数，详见[Source common Options]（../source-common-options.md）

## 示例

简单的例子

```hocon
Easysearch {
    hosts = ["localhost:9200"]
    index = "seatunnel-*"
    source = ["_id","name","age"]
    query = {"range":{"firstPacket":{"gte":1700407367588,"lte":1700407367588}}}
}
```

复杂的例子

```hocon
Easysearch {
    hosts = ["Easysearch:9200"]
    index = "st_index"
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
    query = {"range":{"firstPacket":{"gte":1700407367588,"lte":1700407367588}}}
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

### 下个版本

- 添加 Easysearch source连接器
- 支持https协议
- 支持DSL

