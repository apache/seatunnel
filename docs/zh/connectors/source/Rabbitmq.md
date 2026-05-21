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
| queue_name                 | string  | 否  | -     | 要消费消息的队列                                                                    |
| schema                     | config  | 否  | -     | 上游数据的模式。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。 |
| tables_configs             | array   | 否  | -     | 用于同时从多个队列读取消息。数组中的每个对象必须包含 queue_name 和 schema。                            |
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

要消费消息的队列。*注意：如果未配置 `tables_configs`，则为必填项。*

### routing_key [string]

要发布消息的路由密钥

### exchange [string]

要发布消息的交换机

### schema [Config]

#### fields [Config]

上游数据的模式字段。更多详情请参考 [Schema 特性](../../introduction/concepts/schema-feature.md)。*注意：如果未配置 `tables_configs`，则为必填项。*

### tables_configs [array]

用于同时从多个队列读取消息。数组中的每个对象必须包含 queue_name 和 schema。

### network_recovery_interval [int]

自动恢复在尝试重新连接之前等待多长时间（毫秒）

### topology_recovery_enabled [boolean]

如果为 true，启用拓扑恢复

### automatic_recovery_enabled [boolean]

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

## 迁移指南与配置规则

如果您从仅支持单表读取的早期版本升级，您现有的配置无需任何更改即可正常工作。

**配置优先级：**
- 您不能同时配置 `tables_configs` 和根级别的 `queue_name`/`schema`。它们是互斥的。这样做将导致配置验证错误。
- 使用 `tables_configs` 进行多表模式。
- 使用根级别的 `queue_name` 和 `schema` 进行单表模式。

## 示例

### 单表读取示例

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
### 多表读取示例
您可以使用 `tables_configs` 选项在一个作业中同时从多个 RabbitMQ 队列消费消息。连接器将根据消息来源的队列自动为每行数据分配正确的表标识符，允许您使用 `plugin_input` 将它们路由到不同的 sink。

```hocon
source {
  RabbitMQ {
    host = "localhost"
    port = 5672
    username = "guest"
    password = "guest"

    # 使用 tables_configs 从多个队列中读取
    tables_configs = [
      {
        queue_name = "users_queue"
        schema = {
          table = "users_table" # 定义用于路由的表名
          fields {
            user_id = bigint
            name = string
          }
        }
      },
      {
        queue_name = "orders_queue"
        schema = {
          table = "orders_table" # 定义用于路由的表名
          fields {
            order_id = bigint
            amount = double
          }
        }
      }
    ]
  }
}

sink {
  # 第一个 sink 将仅接收来自 users_queue 的数据
  Jdbc {
    plugin_input = "users_table"
    driver = "com.mysql.cj.jdbc.Driver"
    url = "jdbc:mysql://localhost:3306/mydb"
    query = "insert into users (user_id, name) values (?, ?)"
  }

  # 第二个 sink 将仅接收来自 orders_queue 的数据
  Jdbc {
    plugin_input = "orders_table"
    driver = "com.mysql.cj.jdbc.Driver"
    url = "jdbc:mysql://localhost:3306/mydb"
    query = "insert into orders (order_id, amount) values (?, ?)"
  }
}
```

## 变更日志

<ChangeLog />
