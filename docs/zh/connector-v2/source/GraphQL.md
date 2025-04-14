import ChangeLog from '../changelog/connector-graphql.md';

# GraphQL

> GraphQL Source 连接器

## 描述

用于读取GraphQL数据。

## 主要特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [并行](../../concept/connector-v2-features.md)

## 源选项

| 名称                        | 类型    | 是否必填 | 默认值                  |
| --------------------------- | ------- | -------- | ----------------------- |
| url                         | String  | Yes      | -                       |
| query                       | String  | Yes      | -                       |
| variables                   | Config  | No       | -                       |
| enable_subscription         | boolean | No       | false                   |
| timeout                     | Long    | No       | -                       |
| content_field               | String  | Yes      | $.data.{query_object}.* |
| schema.fields               | Config  | Yes      | -                       |
| format                      | String  | No       | json                    |
| params                      | Map     | Yes      | -                       |
| poll_interval_millis        | int     | No       | -                       |
| retry                       | int     | No       | -                       |
| retry_backoff_multiplier_ms | int     | No       | 100                     |
| retry_backoff_max_ms        | int     | No       | 10000                   |
| enable_multi_lines          | boolean | No       | false                   |
| common-options              | config  | No       | -                       |

### url [String]

http 请求路径。

### query [String]

GraphQL 表达式查询字符串

### variables [String]

GraphQL 变量

比如

```
variables = {
   limit = 2
}
```

### enable_subscription [boolean]

1. true :  构建一个套接字读取器来订阅GraphQL服务
2. false :  构建GraphQL服务的http阅读器订阅

### timeout [Long]

超时时间

### content_field [String]

SONPath通配符

### params [Map]

HTTP请求参数

### poll_interval_millis [int]

流模式下请求HTTP API间隔（毫秒）

### retry [int]

如果请求http返回到‘ IOException ’的最大重试次数

### retry_backoff_multiplier_ms [int]

如果请求http失败，则重试回退时间（毫秒）倍率

### retry_backoff_max_ms [int]

如果http请求失败，最大重试回退时间（毫秒）

### format [String]

上游数据的格式，默认为json。

### schema [Config]

填写一个固定值

```hocon
    schema = {
        fields {
            metric = "map<string, string>"
            value = double
            time = long
            }
        }

```

#### fields [Config]

上游数据的模式字段

### common options

源插件常用参数，请参考 [Source Common Options](../source-common-options.md) 获取详细信息

## 示例

### Query

```hocon
source {
    GraphQL {
        url = "http://192.168.1.103:9081/v1/graphql"
        format = "json"
        content_field = "$.data.source"
        query = """
            query MyQuery($limit: Int) {
                source(limit: $limit) {
                    id
                    val_bool
                    val_double
                    val_float
                }
            }
        """
        variables = {
            limit = 2
        }
        schema = {
            fields {
               id = "int"
               val_bool = "boolean"
               val_double = "double"
               val_float = "float"
            }
        }
    }
}
```

### Subscription

```hocon
source {
    GraphQL {
        url = "http://192.168.1.103:9081/v1/graphql"
        format = "json"
        content_field = "$.data.source"
        query = """
            query MyQuery($limit: Int) {
                source(limit: $limit) {
                    id
                    val_bool
                    val_double
                    val_float
                }
            }
        """
        variables = {
            limit = 2
        }
        enable_subscription = true
        schema = {
            fields {
               id = "int"
               val_bool = "boolean"
               val_double = "double"
               val_float = "float"
            }
        }
    }
}
```

## 变更日志

<ChangeLog />
