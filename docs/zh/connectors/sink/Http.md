import ChangeLog from '../changelog/connector-http.md';

# Http

> Http Sink 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 描述

接收Source端传入的数据，利用数据触发 web hooks。

> 例如，来自上游的数据为[`age: 12, name: tyrantlucifer`]，则body内容如下：`{"age": 12, "name": "tyrantlucifer"}`

**Tips: Http 接收器仅支持 `post json` 类型的 web hook，source 数据将被视为 webhook 中的 body 内容。**

## 支持的数据源信息

想使用 Http 连接器，需要安装以下必要的依赖。可以通过运行 install-plugin.sh 脚本或者从 Maven 中央仓库下载这些依赖

| 数据源  | 支持版本 | 依赖                                                                           |
|------|------|------------------------------------------------------------------------------|
| Http | 通用   | [下载](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-http) |

## 接收器选项

|             名称              |   类型   | 是否必须 |  默认值  |                             描述                             |
|-----------------------------|--------|------|-------|------------------------------------------------------------|
| url                         | String | 是    | -     | Http 请求链接                                                  |
| headers                     | Map    | 否    | -     | Http 请求头                                                    |
| params                      | Map    | 否    | -     | 该参数会通过参数校验。当前 Sink 写入器会把数据作为请求体 POST 到最终 URL，如需查询参数，建议直接写在 `url` 中。 |
| retry                       | Int    | 否    | -     | 如果请求http返回`IOException`的最大重试次数                             |
| retry_backoff_multiplier_ms | Int    | 否    | 100   | http请求失败，重试回退次数（毫秒）乘数                                      |
| retry_backoff_max_ms        | Int    | 否    | 10000 | http请求失败，最大重试回退时间(毫秒)                                      |
| array_mode                  | Boolean| 否    | false | 为true时将数据作为JSON数组发送，为false时作为单个JSON对象发送（默认）                |
| batch_size                  | Int    | 否    | 1     | 在一个HTTP请求中发送的记录批量大小。仅在array_mode为true时有效                   |
| request_interval_ms         | Int    | 否    | 0     | 两次HTTP请求之间的间隔毫秒数，以避免请求过于频繁                                 |
| multi_table_sink_replica    | Int    | 否    | -     | 多表写入时使用的 Sink 副本数，详情请参考 [Sink 常用选项](../common-options/sink-common-options.md)。 |
| common-options              |        | 否    | -     | Sink插件常用参数，请参考 [Sink常用选项 ](../common-options/sink-common-options.md) 了解详情 |

## 示例

Http Sink 固定发送 `POST` 请求。每条上游数据会被转换成 JSON 作为请求体；当 `array_mode = true` 时，会先把多条数据攒成 JSON 数组再发送，`batch_size` 控制单次请求最多包含多少条数据。

简单示例:

```hocon
Http {
    url = "http://localhost/test/webhook"
    headers {
        token = "9e32e859ef044462a257e1fc76730066"
    }
}
```

### 带批处理的示例

```hocon
Http {
    url = "http://localhost/test/webhook"
    headers {
        token = "9e32e859ef044462a257e1fc76730066"
        Content-Type = "application/json"
    }
    array_mode = true
    batch_size = 50
    request_interval_ms = 500
}
```

## 变更日志

<ChangeLog />
