# INFINI Easysearch

## 支持以下引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

一个使用将数据发送到 `INFINI Easysearch` 的接收器插件.

## 使用依赖

> 依赖 [easysearch-client](https://central.sonatype.com/artifact/com.infinilabs/easysearch-client)
>
  ## 关键特性

- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

:::提示

支持的引擎

* 支持 [INFINI Easysearch](https://www.infini.com/download/?product=easysearch) 发布的所有版本.

:::

## 数据类型映射

| Easysearch 数据类型             | SeaTunnel 数据类型   |
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

## 接收器选项

|          名称           |  类型  | 必需 | 默认值 |
|-------------------------|---------|----|---------------|
| hosts                   | array   | 是  | -             |
| index                   | string  | 是  | -             |
| primary_keys            | list    | 否  |               |
| key_delimiter           | string  | 否 | `_`           |
| username                | string  | 否 |               |
| password                | string  | 否 |               |
| max_retry_count         | int     | 否 | 3             |
| max_batch_size          | int     | 否 | 10            |
| tls_verify_certificate  | boolean | 否 | true          |
| tls_verify_hostnames    | boolean | 否 | true          |
| tls_keystore_path       | string  | 否 | -             |
| tls_keystore_password   | string  | 否 | -             |
| tls_truststore_path     | string  | 否 | -             |
| tls_truststore_password | string  | 否 | -             |
| common-options          |         | 否 | -             |

### hosts [array]

`INFINI Easysearch` 集群http地址，格式为 `host:port` , 允许指定多个主机.例如 `["host1:9200", "host2:9200"]`.

### index [string]

`INFINI Easysearch`  `index` 名称.索引支持包含字段名变量,例如 `seatunnel_${age}`,该字段必须出现在seatunnel行.
如果没有，我们将把它当作一个正常的索引.

### primary_keys [list]

用于生成文档 `_id`的主键字段，这是cdc必需的选项.

### key_delimiter [string]

复合键的分隔符 (默认为"_" ), 例如, "$" 将导致文档 `_id` "KEY1$KEY2$KEY3".

### username [string]

安全用户名

### password [string]

安全密码

### max_retry_count [int]

一个批量请求的最大尝试大小

### max_batch_size [int]

批量文档最大大小

### tls_verify_certificate [boolean]

为HTTPS端点启用证书验证

### tls_verify_hostname [boolean]

为HTTPS端点启用主机名验证

### tls_keystore_path [string]

PEM或JKS密钥存储的路径。运行SeaTunnel的操作系统用户必须能够读取此文件.

### tls_keystore_password [string]

指定密钥存储的密钥密码

### tls_truststore_path [string]

PEM或JKS信任存储的路径。运行SeaTunnel的操作系统用户必须能够读取此文件.

### tls_truststore_password [string]

指定的信任存储的密钥密码

### common options

接收器插件常用参数，详见 [Sink Common Options](../sink-common-options.md)

## 示例

简单的例子

```bash
sink {
    Easysearch {
        hosts = ["localhost:9200"]
        index = "seatunnel-${age}"
    }
}
```

CDC(变更数据捕获) 事件

```bash
sink {
    Easysearch {
        hosts = ["localhost:9200"]
        index = "seatunnel-${age}"
        
        # cdc required options
        primary_keys = ["key1", "key2", ...]
    }
}
```

SSL (禁用证书验证)

```hocon
sink {
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
sink {
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
sink {
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

### 2.3.4 2023-11-16

- 添加 Easysearch 接收器连接器
- 支持http/https协议
- 支持 CD C写入 DELETE/UPDATE/INSERT 事件

