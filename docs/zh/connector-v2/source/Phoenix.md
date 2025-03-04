# Phoenix

> Phoenix 源连接器

## 描述

通过[Jdbc连接器] (Jdbc.md) 读取Phoenix数据.
支持批处理模式和流模式。测试的Phoenix版本是4.xx和5.xx
在底层实现上，通过Phoenix的jdbc驱动程序，执行upstart语句将数据写入HBase.
用Java JDBC连接Phoenix的两种方法。一种是通过JDBC连接到zookeeper，另一种是使用JDBC thin 户端连接到 queryserver.

> 提示：默认情况下，使用（thin）驱动程序jar。如果要使用（thick）驱动程序或Phoenix（thin）驱动程序的其他版本，则需要重新编译jdbc连接器模块

## 关键特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [x] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)

支持查询SQL，可以实现投影效果.

- [ ] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../concept/connector-v2-features.md)

## 选项

### driver [string]

如果使用phoenix（thick）驱动程序，则值为`org.apache.phoenix.jdbc.PhoenixDriver` 或您使用的（thin）驱动程序的值是 `org.apache.phoenix.queryserver.client.Driver`

### url [string]

如果您使用phoenix（thick）驱动程序，则值为 `jdbc:phoenix:localhost:2182/hbase` ，或者您使用（thin）驱动程序时，值为 `jdbc:phoenix:thin:url=http://localhost:8765;serialization=PROTOBUF`
### common options

源插件常用参数，详见 [Source Common Options](../source-common-options.md) 

## 示例

使用 thick 客户端驱动器

```
    Jdbc {
        driver = org.apache.phoenix.jdbc.PhoenixDriver
        url = "jdbc:phoenix:localhost:2182/hbase"
        query = "select age, name from test.source"
    }

```

使用 thin 客户端驱动器

```
Jdbc {
    driver = org.apache.phoenix.queryserver.client.Driver
    url = "jdbc:phoenix:thin:url=http://spark_e2e_phoenix_sink:8765;serialization=PROTOBUF"
    query = "select age, name from test.source"
}
```

## 变更日志

### 2.2.0-beta 2022-09-26

- 添加 Phoenix 源连接器

