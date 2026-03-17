import ChangeLog from '../changelog/connector-rabbitmq.md';

# Rabbitmq

> Rabbitmq 源连接器

## 描述

用于从 Rabbitmq 读取数据。

## 关键特性

- [ ] [批](../../introduction/concepts/connector-v2-features.md)
- [x] [流](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

:::tip

为了实现精确一次，源必须是非并行的（并行度设置为 1）。这个限制主要是由于 RabbitMQ 从单个队列向多个消费者分派消息的方式。

:::

## 选项

| 参数名                        | 类型      | 必须 | 默认值   | 描述                                                                          |
|----------------------------|---------|----|-------|-----------------------------------------------------------------------------|
| host                       | string  | 是  | -     | 连接的默认主机                                                                     |
| port                       | int     | 是  | -     | 连接的默认端口                                                                     |
| virtual_host               | string  | 是  | -     | 虚拟主机 – 连接到代理时使用的虚拟主机                                                        |
| username                   | string  | 是  | -     | 连接到代理时使用的 AMQP 用户名                                                          |
| password                   | string  | 是  | -     | 连接到代理时使用的密码                                                                 |
| queue_name                 | string  | 是  | -     | 要发布消息的队列                                                                    |
| schema                     | config  | 是  | -     | 上游数据的模式。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。 |
| url                        | string  | 否  | -     | 便捷方法，用于设置 AMQP URI 中的字段：主机、端口、用户名、密码和虚拟主机                                   |
| routing_key                | string  | 否  | -     | 要发布消息的路由密钥                                                                  |
| exchange                   | string  | 否  | -     | 要发布消息的交换机                                                                   |
| network_recovery_interval  | int     | 否  | -     | 自动恢复在尝试重新连接之前等待多长时间（毫秒）                                                     |
| topology_recovery_enabled  | boolean | 否  | -     | 如果为 true，启用拓扑恢复                                                             |
| automatic_recovery_enabled | boolean | 否  | -     | 如果为 true，启用连接恢复                                                             |
| connection_timeout         | int     | 否  | -     | 连接 tcp 建立超时（毫秒）；零表示无限                                                       |
| requested_channel_max      | int     | 否  | -     | 最初请求的最大通道数；零表示无限制。**注意：值必须在 0 到 65535 之间（AMQP 0-9-1 中的无符号短整数）。              |
| requested_frame_max        | int     | 否  | -     | 请求的最大帧大小                                                                    |
| requested_heartbeat        | int     | 否  | -     | 设置请求的心跳超时。**注意：值必须在 0 到 65535 之间（AMQP 0-9-1 中的无符号短整数）。                      |
| prefetch_count             | int     | 否  | -     | 预取计数，无需确认即可接收的最大消息数                                                         |
| delivery_timeout           | long    | 否  | -     | 交付超时，等待下一条消息交付的最大时间（毫秒）                                                     |
| durable                    | boolean | 否  | true  | 队列是否在服务器重启时保留                                                               |
| exclusive                  | boolean | 否  | false | 队列是否仅由当前连接使用                                                                |
| auto_delete                | boolean | 否  | false | 队列是否在最后一个消费者取消订阅时自动删除                                                       |
| common-options             |         | 否  | -     | 源插件通用参数                                                                     |

### host [string]

连接的默认主机

### port [int]

连接的默认端口

### virtual_host [string]

虚拟主机 – 连接到代理时使用的虚拟主机

### username [string]

连接到代理时使用的 AMQP 用户名

### password [string]

连接到代理时使用的密码

### url [string]

便捷方法，用于设置 AMQP URI 中的字段：主机、端口、用户名、密码和虚拟主机

### queue_name [string]

要发布消息的队列

### routing_key [string]

要发布消息的路由密钥

### exchange [string]

要发布消息的交换机

### schema [Config]

#### fields [Config]

上游数据的模式字段。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。

### network_recovery_interval [int]

自动恢复在尝试重新连接之前等待多长时间（毫秒）

### topology_recovery_enabled [string]

如果为 true，启用拓扑恢复

### automatic_recovery_enabled [string]

如果为 true，启用连接恢复

### connection_timeout [int]

连接 tcp 建立超时（毫秒）；零表示无限

### requested_channel_max [int]

最初请求的最大通道数；零表示无限制。**注意：值必须在 0 到 65535 之间（AMQP 0-9-1 中的无符号短整数）。

### requested_frame_max [int]

请求的最大帧大小

### requested_heartbeat [int]

设置请求的心跳超时。**注意：值必须在 0 到 65535 之间（AMQP 0-9-1 中的无符号短整数）。

### prefetch_count [int]

预取计数，无需确认即可接收的最大消息数

### delivery_timeout [long]

交付超时，等待下一条消息交付的最大时间（毫秒）

### common options

源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。

### durable

- true：队列将在服务器重启时保留。
- false：队列将在服务器重启时删除。

### exclusive

- true：队列仅由当前连接使用，连接关闭时将删除。
- false：队列可以由多个连接使用。

### auto-delete

- true：队列将在最后一个消费者取消订阅时自动删除。
- false：队列不会自动删除。

## 示例

简单：

```hocon
source {
    RabbitMQ {
        host = "rabbitmq-e2e"
        port = 5672
        virtual_host = "/"
        username = "guest"
        password = "guest"
        queue_name = "test"
        schema = {
            fields {
                id = bigint
                c_map = "map<string, smallint>"
                c_array = "array<tinyint>"
            }
        }
    }
}
```

## 变更日志

<ChangeLog />

