# Rabbitmq

> Rabbitmq 数据接收器

## 描述

该数据接收器是将数据写入Rabbitmq。

## 主要特性

- [ ] [精准一次](../../concept/connector-v2-features.md)

## 接收器选项

|             名称             |   类型    | 是否必须 |  默认值  |
|----------------------------|---------|------|-------|
| host                       | string  | yes  | -     |
| port                       | int     | yes  | -     |
| virtual_host               | string  | yes  | -     |
| username                   | string  | yes  | -     |
| password                   | string  | yes  | -     |
| queue_name                 | string  | yes  | -     |
| url                        | string  | no   | -     |
| network_recovery_interval  | int     | no   | -     |
| topology_recovery_enabled  | boolean | no   | -     |
| automatic_recovery_enabled | boolean | no   | -     |
| use_correlation_id         | boolean | no   | false |
| connection_timeout         | int     | no   | -     |
| rabbitmq.config            | map     | no   | -     |
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

### url [string]

设置host、port、username、password和virtual host的简便方式。

### queue_name [string]

数据写入的队列名。

### schema [Config]

#### fields [Config]

上游数据的模式字段。

### network_recovery_interval [int]

自动恢复需等待多长时间才尝试重连，单位为毫秒。

### topology_recovery_enabled [boolean]

设置为true，表示启用拓扑恢复。

### automatic_recovery_enabled [boolean]

设置为true，表示启用连接恢复。

### use_correlation_id [boolean]

接收到的消息是否都提供唯一ID，来删除重复的消息达到幂等（在失败的情况下）

### connection_timeout [int]

TCP连接建立的超时时间，单位为毫秒；0代表不限制。

### rabbitmq.config [map]

In addition to the above parameters that must be specified by the RabbitMQ client, the user can also specify multiple non-mandatory parameters for the client, covering [all the parameters specified in the official RabbitMQ document](https://www.rabbitmq.com/configure.html).
除了上面提及必须设置的RabbitMQ客户端参数，你也还可以为客户端指定多个非强制参数，参见 [RabbitMQ官方文档参数设置](https://www.rabbitmq.com/configure.html)。

### common options

Sink插件常用参数，请参考[Sink常用选项](../sink-common-options.md)获取更多细节信息。

## 示例

simple:

```hocon
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

## 变更日志

### 随后版本

- 增加Rabbitmq数据接收器
- [Improve] 将连接器自定义配置前缀的数据类型更改为Map [3719](https://github.com/apache/seatunnel/pull/3719)

