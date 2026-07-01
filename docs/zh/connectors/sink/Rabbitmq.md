import ChangeLog from '../changelog/connector-rabbitmq.md';

# Rabbitmq

> Rabbitmq 数据接收器

## 描述

该数据接收器是将数据写入Rabbitmq。

## 主要特性

- [ ] [精准一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## 接收器选项

|             名称             |   类型    | 是否必须 |  默认值  |
|----------------------------|---------|------|-------|
| host                       | string  | yes  | -     |
| port                       | int     | yes  | -     |
| virtual_host               | string  | yes  | -     |
| username                   | string  | no   | -     |
| password                   | string  | no   | -     |
| queue_name                 | string  | yes  | -     |
| url                        | string  | no   | -     |
| routing_key                | string  | no   | -     |
| exchange                   | string  | no   | -     |
| network_recovery_interval  | int     | no   | -     |
| topology_recovery_enabled  | boolean | no   | -     |
| AUTOMATIC_RECOVERY_ENABLED | boolean | no   | -     |
| connection_timeout         | int     | no   | -     |
| rabbitmq.config            | map     | no   | -     |
| durable                    | boolean | no   | true  |
| exclusive                  | boolean | no   | false |
| auto_delete                | boolean | no   | false |
| common-options             |         | no   | -     |

### host [string]

Rabbitmq服务器地址

### port [int]

Rabbitmq服务器端口

### virtual_host [string]

virtual host – 连接broker使用的vhost

### username [string]

连接broker时使用的用户名

### password [string]

连接broker时使用的密码

`username` 和 `password` 需要一起配置。

### url [string]

设置host、port、username、password和virtual host的简便方式。

### queue_name [string]

数据写入的队列名。如果没有配置 `routing_key`，连接器会通过默认 exchange 将消息直接写入该队列。

### routing_key [string]

发布消息时使用的路由键。如果希望通过指定 exchange 发布消息，而不是直接写入 `queue_name`，请同时配置 `routing_key` 和 `exchange`。

### exchange [string]

配置 `routing_key` 时使用的 exchange。

### durable [boolean]

- true：队列将在服务器重启时保留。
- false：队列将在服务器重启时删除。

### exclusive [boolean]

- true：队列仅由当前连接使用，连接关闭时将删除。
- false：队列可以由多个连接使用。

### auto_delete [boolean]

- true：队列将在最后一个消费者取消订阅时自动删除。
- false：队列不会自动删除。

### network_recovery_interval [int]

自动恢复需等待多长时间才尝试重连，单位为毫秒。

### topology_recovery_enabled [boolean]

设置为true，表示启用拓扑恢复。

### AUTOMATIC_RECOVERY_ENABLED [boolean]

设置为true，表示启用连接恢复。

### connection_timeout [int]

TCP连接建立的超时时间，单位为毫秒；0代表不限制。

### rabbitmq.config [map]

In addition to the above parameters that must be specified by the RabbitMQ client, the user can also specify multiple non-mandatory parameters for the client, covering [all the parameters specified in the official RabbitMQ document](https://www.rabbitmq.com/configure.html).
除了上面提及必须设置的RabbitMQ客户端参数，你也还可以为客户端指定多个非强制参数，参见 [RabbitMQ官方文档参数设置](https://www.rabbitmq.com/configure.html)。

### common options

Sink插件常用参数，请参考[Sink常用选项](../common-options/sink-common-options.md)获取更多细节信息。

## 配置说明

- 如果配置了 `username`，也必须配置 `password`，反过来也一样。
- `host`、`port`、`virtual_host` 和 `queue_name` 是连接器必填项。`url` 可额外提供 RabbitMQ 客户端使用的 AMQP URI。
- `durable`、`exclusive` 和 `auto_delete` 用于连接器声明目标队列。

## 示例

simple:

```hocon
env {
    parallelism = 1
    job.mode = "STREAMING"
}

source {
    FakeSource {
        row.num = 10
        schema = {
            fields {
                id = bigint
                c_string = string
            }
        }
    }
}

sink {
      RabbitMQ {
          host = "rabbitmq-e2e"
          port = 5672
          virtual_host = "/"
          username = "guest"
          password = "guest"
          queue_name = "test1"
          rabbitmq.config = {
            requested-heartbeat = 10
            connection-timeout = 10
          }
      }
}
```

### 示例 2

配置队列的 durable、exclusive、auto_delete：

```hocon
env {
    parallelism = 1
    job.mode = "STREAMING"
}

source {
    FakeSource {
        row.num = 10
        schema = {
            fields {
                id = bigint
                c_string = string
            }
        }
    }
}

sink {
      RabbitMQ {
          host = "rabbitmq-e2e"
          port = 5672
          virtual_host = "/"
          username = "guest"
          password = "guest"
          queue_name = "test1"
          durable = "true"
          exclusive = "false"
          auto_delete = "false"
          rabbitmq.config = {
            requested-heartbeat = 10
            connection-timeout = 10
          }
      }
}
```

## 变更日志

<ChangeLog />
