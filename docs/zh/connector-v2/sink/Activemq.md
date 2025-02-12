# Activemq

> Activemq 接收器连接器

## 描述

用于将数据写入 Activemq.

## 关键特性

- [ ] [精确一次](../../concept/connector-v2-features.md)

## 选项

|                名称                 |  类型   | 必需 | 默认值 |
|-------------------------------------|---------|----------|--------------|
| host                                | string  | no       | -            |
| port                                | int     | no       | -            |
| virtual_host                        | string  | no       | -            |
| username                            | string  | no       | -            |
| password                            | string  | no       | -            |
| queue_name                          | string  | yes      | -            |
| uri                                 | string  | yes      | -            |
| check_for_duplicate                 | boolean | no       | -            |
| client_id                           | boolean | no       | -            |
| copy_message_on_send                | boolean | no       | -            |
| disable_timeStamps_by_default       | boolean | no       | -            |
| use_compression                     | boolean | no       | -            |
| always_session_async                | boolean | no       | -            |
| dispatch_async                      | boolean | no       | -            |
| nested_map_and_list_enabled         | boolean | no       | -            |
| warnAboutUnstartedConnectionTimeout | boolean | no       | -            |
| closeTimeout                        | int     | no       | -            |

### host [string]

用于连接的默认主机

### port [int]

the default port to use for connections

### username [string]

用于连接的默认端口

### password [string]

连接到代理时使用的密码

### uri [string]

用于设置 AMQP URI 中字段（主机、端口、用户名、密码和虚拟主机）的便捷方法

### queue_name [string]

写入消息的队列

### check_for_duplicate [boolean]

将检查重复消息

### client_id [string]

客户端ID

### copy_message_on_send [boolean]

如果为true，则启用新的JMS消息对象作为发送方法的一部分

### disable_timeStamps_by_default [boolean]

禁用时间戳以获得轻微的性能提升.

### use_compression [boolean]

允许对消息正文使用压缩.

### always_session_async [boolean]

当为true时，将使用单独的线程为连接中的每个会话分派消息.

### always_sync_send [boolean]

当为true时，MessageProducer在发送消息时将始终使用同步发送

### close_timeout [boolean]

设置关闭完成前的超时时间（以毫秒为单位）.

### dispatch_async [boolean]

代理是否应该异步地向消费者发送消息

### nested_map_and_list_enabled [boolean]

控制是否支持结构化消息属性和MapMessages

### warn_about_unstarted_connection_timeout [int]

从创建连接到生成警告的超时时间（毫秒）

## 示例

简单:

```hocon
sink {
      ActiveMQ {
          uri="tcp://localhost:61616"
          username = "admin"
          password = "admin"
          queue_name = "test1"
      }
}
```

## 变更日志

### 下一个版本

- 添加Activemq源连接器

