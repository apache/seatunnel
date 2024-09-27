# Phoenix

> Phoenix 数据接收器

## 描述

该接收器是通过 [Jdbc数据连接器](Jdbc.md)来写Phoenix数据，支持批和流两种模式。测试的Phoenix版本为4.xx和5.xx。
在底层实现上，通过Phoenix的jdbc驱动，执行upsert语句向HBase写入数据。
使用Java JDBC连接Phoenix有两种方式：其一是使用JDBC连接zookeeper，其二是通过JDBC瘦客户端连接查询服务器。

> 提示1: 该接收器默认使用的是（thin）驱动jar包。如果需要使用（thick）驱动或者其他版本的Phoenix（thin）驱动，需要重新编译jdbc数据接收器模块。
>
> 提示2: 该接收器还不支持精准一次语义（因为Phoenix还不支持XA事务）。

## 主要特性

- [ ] [精准一次](../../concept/connector-v2-features.md)

## 接收器选项

### driver [string]

phoenix（thick）驱动：`org.apache.phoenix.jdbc.PhoenixDriver`
phoenix（thin）驱动：`org.apache.phoenix.queryserver.client.Driver`

### url [string]

phoenix（thick）驱动：`jdbc:phoenix:localhost:2182/hbase`
phoenix（thin）驱动：`jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF`

### common options

Sink插件常用参数，请参考[Sink常用选项](../sink-common-options.md)获取更多细节信息。

## 示例

thick驱动：

```
    Jdbc {
        driver = org.apache.phoenix.jdbc.PhoenixDriver
        url = "jdbc:phoenix:localhost:2182/hbase"
        query = "upsert into test.sink(age, name) values(?, ?)"
    }

```

thin驱动：

```
Jdbc {
    driver = org.apache.phoenix.queryserver.client.Driver
    url = "jdbc:phoenix:thin:url=http://spark_e2e_phoenix_sink:8765;serialization=PROTOBUF"
    query = "upsert into test.sink(age, name) values(?, ?)"
}
```

## 变更日志

### 2.2.0-beta 2022-09-26

- 增加Phoenix数据接收器

