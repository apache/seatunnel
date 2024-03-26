# Http

> Http 数据接收器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [精确一次](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

## 描述

用于发射获取web数据的钩子

> 例如，来自上游的数据为[`age: 12, name: tyrantlucifer`]，则body内容如下：`{"age": 12, "name": "tyrantlucifer"}`

**Tips: Http 接收器仅支持 `post json`  webhook，来自源的数据将被视为 web hook 中的正文内容。**

## 支持的数据源信息

为了使用 Http 连接器，需要以下依赖项。它们可以通过 install-plugin.sh 或从 Maven 中央存储库下载。

| 数据源  | 支持版本 |                                                  依赖                                                  |
|------|------|------------------------------------------------------------------------------------------------------|
| Http | 通用   | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/seatunnel-connectors-v2/connector-http) |

## 接收器选项

|             名称              |   类型   | 是否必须 |  默认值  |                                  描述                                   |
|-----------------------------|--------|------|-------|-----------------------------------------------------------------------|
| url                         | String | 是    | -     | Http 请求链接                                                             |
| headers                     | Map    | 否    | -     | Http 标头                                                               |
| retry                       | Int    | 否    | -     | 如果请求http返回`IOException`的最大重试次数                                        |
| retry_backoff_multiplier_ms | Int    | 否    | 100   | http请求失败，重试回退次数（毫秒）乘数                                                 |
| retry_backoff_max_ms        | Int    | 否    | 10000 | http请求失败，最大重试回退时间(毫秒)                                                 |
| connect_timeout_ms          | Int    | 否    | 12000 | 连接超时设置，默认12s                                                          |
| socket_timeout_ms           | Int    | 否    | 60000 | 套接字超时设置，默认为60s                                                        |
| common-options              |        | 否    | -     | Sink插件常用参数，请参考 [Sink常用选项 ](../../transform-v2/common-options.md) 了解详情 |

## 示例

简单示例:

```hocon
Http {
    url = "http://localhost/test/webhook"
    headers {
        token = "9e32e859ef044462a257e1fc76730066"
    }
}
```

## 变更日志

### 2.2.0-beta 2022-09-26

- 添加Http接收连接器

