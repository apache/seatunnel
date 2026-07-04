import ChangeLog from '../changelog/connector-easysearch.md';

# INFINI Easysearch

## 支持以下引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于将数据写入 `INFINI Easysearch`。

## 使用依赖

> 依赖 [easysearch-client](https://central.sonatype.com/artifact/com.infinilabs/easysearch-client)
## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [x] [批处理](../../introduction/concepts/connector-v2-features.md)

:::提示

支持的引擎

* 支持 [INFINI Easysearch](https://www.infini.com/download/?product=easysearch) 发布的所有版本。

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
| DATE                        | LOCAL_DATE_TIME_TYPE |

## 接收器选项

|          名称           | 类型    | 是否必填 | 默认值 |
|------------------------|---------|----|---------------|
| hosts                  | array   | 是  | -             |
| index                  | string  | 是  | -             |
| primary_keys           | list    | 否       | -             |
| key_delimiter          | string  | 否       | `_`           |
| username               | string  | 否       | -             |
| password               | string  | 否       | -             |
| max_retry_count        | int     | 否       | 3             |
| max_batch_size         | int     | 否       | 10            |
| tls_verify_certificate | boolean | 否       | true          |
| tls_verify_hostname    | boolean | 否       | true          |
| tls_keystore_path      | string  | 否       | -             |
| tls_keystore_password  | string  | 否       | -             |
| tls_truststore_path    | string  | 否       | -             |
| tls_truststore_password | string | 否       | -             |
| schema_save_mode       | enum    | 否       | CREATE_SCHEMA_WHEN_NOT_EXIST |
| data_save_mode         | enum    | 否       | APPEND_DATA   |
| common-options         | config  | 否       | -             |

### hosts [array]

`INFINI Easysearch` 集群 HTTP 地址，格式为 `host:port`，允许指定多个主机，例如 `["host1:9200", "host2:9200"]`。

### index [string]

`INFINI Easysearch` 索引名称。索引名可以包含字段占位符，例如 `seatunnel_${age}`。引用的字段必须存在于输入行中；如果不存在，会按普通索引名处理。

### primary_keys [list]

用于生成文档 `_id` 的主键字段。写入需要更新或删除语义的 CDC 数据时需要配置。

### key_delimiter [string]

复合键的分隔符 (默认为"_" ), 例如, "$" 将导致文档 `_id` "KEY1$KEY2$KEY3".

### username [string]

安全用户名

### password [string]

安全密码

### max_retry_count [int]

单次批量请求的最大重试次数。

### max_batch_size [int]

单次批量请求最多缓存的文档数量。

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

### schema_save_mode [enum]

在启动同步任务之前，针对目标侧已有的表结构选择不同的处理方案：
- `RECREATE_SCHEMA`：当表不存在时会创建，当表已存在时会删除并重建
- `CREATE_SCHEMA_WHEN_NOT_EXIST`：当表不存在时会创建，当表已存在时则跳过创建
- `ERROR_WHEN_SCHEMA_NOT_EXIST`：当表不存在时将抛出错误
- `IGNORE`：忽略对表的处理

### data_save_mode [enum]

在启动同步任务之前，针对目标端已有的数据选择不同的处理方案：
- `DROP_DATA`：保留数据库结构并删除数据
- `APPEND_DATA`：保留数据库结构，保留数据
- `ERROR_WHEN_DATA_EXISTS`：有数据时报错

### common options

Sink 插件通用参数，详见 [Sink Common Options](../common-options/sink-common-options.md)。

## 示例

### 写入固定索引

```hocon
sink {
  Easysearch {
    hosts = ["localhost:9200"]
    index = "seatunnel_index"
    max_batch_size = 100
  }
}
```

### 写入动态索引

```hocon
sink {
  Easysearch {
    hosts = ["localhost:9200"]
    index = "seatunnel_${age}"
  }
}
```

### CDC 事件

```hocon
sink {
  Easysearch {
    hosts = ["localhost:9200"]
    index = "seatunnel_${age}"
    primary_keys = ["key1", "key2"]
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

配置表生成策略

```hocon
sink {
    Easysearch {
        hosts = ["https://localhost:9200"]
        username = "admin"
        password = "admin"

        index = "seatunnel_index"
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode = "APPEND_DATA"
    }
}
```

## 变更日志

<ChangeLog />
