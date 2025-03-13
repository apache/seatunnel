# Elasticsearch

## 描述

输出数据到 `Elasticsearch`

## 主要特性

- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

:::tip

引擎支持

* 支持  `ElasticSearch 版本 >= 2.x 并且 <= 8.x`

:::

## 选项

|           名称            |   类型    | 是否必须 |             默认值              |
|-------------------------|---------|------|------------------------------|
| hosts                   | array   | 是    | -                            |
| index                   | string  | 是    | -                            |
| schema_save_mode        | string  | 是    | CREATE_SCHEMA_WHEN_NOT_EXIST |
| data_save_mode          | string  | 是    | APPEND_DATA                  |
| index_type              | string  | 否    |                              |
| primary_keys            | list    | 否    |                              |
| key_delimiter           | string  | 否    | `_`                          |
| username                | string  | 否    |                              |
| password                | string  | 否    |                              |
| max_retry_count         | int     | 否    | 3                            |
| max_batch_size          | int     | 否    | 10                           |
| tls_verify_certificate  | boolean | 否    | true                         |
| tls_verify_hostnames    | boolean | 否    | true                         |
| tls_keystore_path       | string  | 否    | -                            |
| tls_keystore_password   | string  | 否    | -                            |
| tls_truststore_path     | string  | 否    | -                            |
| tls_truststore_password | string  | 否    | -                            |
| common-options          |         | 否    | -                            |

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

### username [string]

x-pack 用户名

### password [string]

x-pack 密码

### max_retry_count [int]

批次批量请求最大尝试大小

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

Sink插件常用参数，请参考 [Sink常用选项](../sink-common-options.md) 了解详情

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

配置表生成策略 (schema_save_mode)

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
    base-url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
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
