import ChangeLog from '../changelog/connector-activemq.md';

# ActiveMQ

> ActiveMQ Sink 连接器

## 描述

用于把 SeaTunnel 数据写入 ActiveMQ 队列。每一行数据都会被序列化成一条 JSON 文本消息。这个连接器只支持
Sink，SeaTunnel 目前没有提供 ActiveMQ Source 连接器。

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称                                    | 类型      | 是否必传 | 默认值 | 描述                                                                                                      |
|---------------------------------------|---------|------|-----|---------------------------------------------------------------------------------------------------------|
| uri                                   | string  | 是    | -   | ActiveMQ Broker 地址，例如 `tcp://localhost:61616`。                                                           |
| queue_name                            | string  | 是    | -   | 要写入的队列名称。                                                                                              |
| username                              | string  | 否    | -   | 创建 ActiveMQ 连接时使用的用户名。配置该项时必须同时配置 `password`。                                                      |
| password                              | string  | 否    | -   | 创建 ActiveMQ 连接时使用的密码。配置该项时必须同时配置 `username`。                                                      |
| client_id                             | string  | 否    | -   | 连接工厂使用的 JMS 客户端 ID。                                                                                   |
| check_for_duplicate                   | boolean | 否    | -   | 是否让 ActiveMQ 客户端检查重复消息。                                                                               |
| always_session_async                  | boolean | 否    | -   | 是否始终为每个 Session 使用独立线程分发消息。                                                                          |
| always_sync_send                      | boolean | 否    | -   | 是否始终使用同步方式发送消息。                                                                                       |
| close_timeout                         | int     | 否    | -   | 关闭连接的超时时间，单位毫秒。                                                                                        |
| dispatch_async                        | boolean | 否    | -   | Broker 是否异步分发消息。                                                                                       |
| nested_map_and_list_enabled           | boolean | 否    | -   | 是否允许结构化消息属性和 `MapMessage` 条目中包含嵌套的 `Map`、`List` 对象。                                               |
| warn_about_unstarted_connection_timeout | int   | 否    | -   | 连接没有正确启动时，ActiveMQ 客户端发出警告前等待的毫秒数。设置为小于 `0` 的值可以关闭这个警告。                              |
| consumer_expiry_check_enabled           | boolean | 否    | -   | 是否在每个 `MessageConsumer` 分发消息前检查消息是否已经过期。                                                  |

## 注意事项

- `uri` 是连接入口，Broker 的主机和端口都写在这里，例如 `tcp://activemq-host:61616`。
- `username` 和 `password` 是可选项；如果 Broker 需要认证，这两个配置要一起写。
- 连接器会把每一行 SeaTunnel 数据作为一条 JSON 文本消息写入 `queue_name`，当前没有单独的 `format` 配置。
- Broker 地址请使用 `uri` 配置，`host` 和 `port` 不是 ActiveMQ Sink 的配置项。
- 该 Sink 前面可以接任意 SeaTunnel Source。ActiveMQ 连接器只负责把最终的数据行发送到队列。

## 示例

把 FakeSource 数据写入 ActiveMQ 队列：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        id = int
        name = string
      }
    }
    rows = [
      { kind = INSERT, fields = [1, "Alice"] }
      { kind = INSERT, fields = [2, "Bob"] }
    ]
  }
}

sink {
  ActiveMQ {
    uri = "tcp://localhost:61616"
    username = "admin"
    password = "admin"
    queue_name = "testQueue"
  }
}
```

在流式模式下，Sink 会保持与 Broker 的连接持续打开，每来一行数据就写入一条。用户名 / 密码也可以直接
写在 `uri` 中，例如 `tcp://admin:admin@localhost:61616`：

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  FakeSource {
    schema = {
      fields {
        id = int
        name = string
      }
    }
    rows = [
      { kind = INSERT, fields = [1, "Alice"] }
    ]
  }
}

sink {
  ActiveMQ {
    uri = "tcp://admin:admin@localhost:61616"
    queue_name = "testQueue"
  }
}
```

## 变更日志

<ChangeLog />
