import ChangeLog from '../changelog/connector-jms.md';

# Jms

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

> 通用 JMS 队列数据源连接器

## 描述

使用消息代理自身的客户端库，从任何提供 JMS 1.1（`javax.jms`）客户端的消息代理读取一个队列中的消息。例如 ActiveMQ Artemis、ActiveMQ Classic、IBM MQ、TIBCO EMS 和 Solace；AMQP 1.0 代理通过 Apache Qpid JMS 客户端访问。

经过测试的提供方列在下文“安装 JMS 客户端”一节中。其他提供方预期可以通过其 `javax.jms` 客户端工作，但未经测试。

本连接器不替代 [ActiveMQ](Activemq.md) 或 [RabbitMQ](Rabbitmq.md) 连接器，二者保留各自的特有功能。

## 支持的引擎

> Flink<br/>
> SeaTunnel Zeta<br/>

数据源要求启用检查点的流作业。不支持 Spark。

## 主要特性

- [ ] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 安装 JMS 客户端

连接器只携带 JMS 1.1 API（`geronimo-jms_1.1_spec` 1.1.1，Apache License 2.0）。各代理客户端库的许可证和版本不同，因此不随连接器打包。请把代理的客户端 jar 放到每个运行 SeaTunnel 的节点的 `${SEATUNNEL_HOME}/plugins/connector-jms/` 目录中。该目录中的 jar 只为本连接器加载。

下面列出的是 Maven 为每个已测试客户端解析出的运行时依赖。

**ActiveMQ Artemis，core 协议**（`org.apache.activemq:artemis-jms-client:2.19.1`）。端到端测试安装到 `plugins/connector-jms/` 的正是这组 jar：

```
artemis-jms-client-2.19.1.jar
artemis-core-client-2.19.1.jar
artemis-commons-2.19.1.jar
artemis-selector-2.19.1.jar
jakarta.jms-api-2.0.3.jar
jakarta.json-api-1.1.6.jar
johnzon-core-0.9.5.jar
jboss-logging-3.4.2.Final.jar
jgroups-3.6.13.Final.jar
commons-beanutils-1.9.4.jar
commons-collections-3.2.2.jar
commons-logging-1.2.jar
netty-buffer-4.1.73.Final.jar
netty-codec-4.1.73.Final.jar
netty-codec-http-4.1.73.Final.jar
netty-codec-socks-4.1.73.Final.jar
netty-common-4.1.73.Final.jar
netty-handler-4.1.73.Final.jar
netty-handler-proxy-4.1.73.Final.jar
netty-resolver-4.1.73.Final.jar
netty-transport-4.1.73.Final.jar
netty-transport-classes-epoll-4.1.73.Final.jar
netty-transport-classes-kqueue-4.1.73.Final.jar
netty-transport-native-epoll-4.1.73.Final-linux-x86_64.jar
netty-transport-native-kqueue-4.1.73.Final-osx-x86_64.jar
netty-transport-native-unix-common-4.1.73.Final.jar
netty-tcnative-classes-2.0.46.Final.jar
```

**ActiveMQ Classic，OpenWire**（`org.apache.activemq:activemq-client:5.15.16`）：

```
activemq-client-5.15.16.jar
hawtbuf-1.11.jar
geronimo-j2ee-management_1.1_spec-1.0.1.jar
geronimo-jms_1.1_spec-1.1.1.jar
slf4j-api-1.7.32.jar
```

**通过 Apache Qpid JMS 访问 AMQP 1.0**（`org.apache.qpid:qpid-jms-client:0.61.0`），已对 ActiveMQ Artemis 测试：

```
qpid-jms-client-0.61.0.jar
proton-j-0.33.10.jar
geronimo-jms_2.0_spec-1.0-alpha-2.jar
slf4j-api-1.7.32.jar
netty-buffer-4.1.72.Final.jar
netty-codec-4.1.72.Final.jar
netty-codec-http-4.1.72.Final.jar
netty-common-4.1.72.Final.jar
netty-handler-4.1.72.Final.jar
netty-resolver-4.1.72.Final.jar
netty-transport-4.1.72.Final.jar
netty-transport-classes-epoll-4.1.72.Final.jar
netty-transport-classes-kqueue-4.1.72.Final.jar
netty-transport-native-epoll-4.1.72.Final-linux-x86_64.jar
netty-transport-native-kqueue-4.1.72.Final-osx-x86_64.jar
netty-transport-native-unix-common-4.1.72.Final.jar
netty-tcnative-classes-2.0.46.Final.jar
```

ActiveMQ Classic 和 Qpid JMS 客户端在连接器单元测试中使用内嵌代理测试；ActiveMQ Artemis 客户端还在端到端测试中针对 `apache/activemq-artemis` 容器测试。

## 配置项

| 名称 | 类型 | 必填 | 默认值 |
| --- | --- | --- | --- |
| connection_factory_class | string | 是，或使用 `jndi_connection_factory_name` | - |
| connection_factory_properties | map | 否 | - |
| jndi_connection_factory_name | string | 是，或使用 `connection_factory_class` | - |
| jndi_initial_context_factory | string | 与 `jndi_connection_factory_name` 一起必填 | - |
| jndi_provider_url | string | 否 | - |
| jndi_properties | map | 否 | - |
| username | string | 否 | - |
| password | string | 否 | - |
| destination | string | 是 | - |
| message_selector | string | 否 | - |
| format | enum | 否 | JSON |
| field_delimiter | string | 否 | , |
| max_in_flight_messages | int | 否 | 1000 |
| schema | config | 是 | - |

`connection_factory_class` 和 `jndi_connection_factory_name` 必须且只能设置一个。另一种方式的配置项会被拒绝。

### connection_factory_class [string]

提供方的 `javax.jms.ConnectionFactory` 实现类，通过其公共无参构造函数创建。例如：`org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory`（ActiveMQ Artemis）、`org.apache.activemq.ActiveMQConnectionFactory`（ActiveMQ Classic）、`org.apache.qpid.jms.JmsConnectionFactory`（Qpid JMS）。

### connection_factory_properties [map]

按键的字母顺序，通过连接工厂的公共 setter 方法设置的属性。键 `brokerURL` 调用 `setBrokerURL`。值会转换为 setter 的参数类型：`String`、`int`、`long`、`boolean` 或它们的包装类型。带点的键对前面的部分调用 getter，对最后一部分调用 setter：`prefetchPolicy.queuePrefetch` 调用 `getPrefetchPolicy().setQueuePrefetch(...)`。

未知的键或无法转换的值会在读取器创建连接工厂时使其失败。错误信息包含键，但不包含值。

代理地址是连接工厂的一个属性，例如 ActiveMQ Artemis 和 ActiveMQ Classic 使用 `brokerURL`，Qpid JMS 使用 `remoteURI`。

### jndi_connection_factory_name / jndi_initial_context_factory / jndi_provider_url / jndi_properties

改为从 JNDI 查找连接工厂。`jndi_initial_context_factory` 是 `java.naming.factory.initial` 类，必填；`jndi_provider_url` 是 `java.naming.provider.url`；`jndi_properties` 用于添加其他环境项。初始上下文工厂类也必须放在 `plugins/connector-jms/` 中。

### username / password [string]

两者都设置时，使用 `createConnection(username, password)` 打开连接；否则使用 `createConnection()`。请把凭据放在这两个配置项中，而不是放在代理 URL 或属性 map 中：它们不会被写入日志，并且[配置加密](../../introduction/configuration/config-encryption-decryption.md)只保护顶层配置项，不处理 map 中的值。

### destination [string]

队列名称，传给 `Session.createQueue`。其语法由提供方定义。不支持主题（topic）。

### message_selector [string]

JMS 消息选择器，由代理执行。不匹配的消息保留在队列中。

### format / field_delimiter

`TextMessage` 的消息体，或按 UTF-8 文本读取的 `BytesMessage` 消息体，按 `schema` 转换。`JSON` 映射一个对象或对象数组。`TEXT` 按 `field_delimiter`（默认 `,`）拆分消息体。

其他消息类型、空消息体或与 schema 不匹配的消息体会使读取器失败，该消息不会被提交。`ObjectMessage` 永远不会被反序列化。错误信息包含 JMS 消息 ID，但不包含消息体。

### max_in_flight_messages [int]

单个读取器已接收但尚未提交的最大消息数。达到上限后，读取器停止接收，直到有检查点完成。由此带来两点：

- 每个检查点间隔内，一个读取器最多读取这么多条消息。默认值 1000、`checkpoint.interval` 为 10 秒时，每个读取器约每秒 100 条。请按预期消息速率设置 `checkpoint.interval`、本配置项和并行度。
- 请使其低于代理对单个事务中未提交消息数的限制，例如 IBM MQ 队列管理器属性 `MaxUncommittedMsgs`。超出该限制的接收会使读取器失败。

### schema [config]

行结构，参见 [Schema 特性](../../introduction/concepts/schema-feature.md)。消息头和消息属性不会作为列输出。

### 公共配置

参见[数据源公共配置](../common-options/source-common-options.md)。

## 交付与恢复

- 每个读取器打开一个连接，并为每个检查点周期使用一个事务会话。检查点封存当前周期的会话；检查点完成时，提交截至该检查点的所有已封存会话。代理会保留每条消息，直到接收它的会话被提交。
- 语义为**至少一次**。失败、取消或恢复后，未提交的会话被回滚，代理重新投递其中的消息，因此可能输出重复行。重复会造成影响时请使用幂等的下游。重新投递或并行读取时不保证顺序。
- 中止的检查点不提交任何消息；下一个完成的检查点会一并提交它的消息。
- 提交失败时读取器失败，作业进行故障转移；在 Zeta 上，只要 `job.retry.times` 允许，作业会从最后完成的检查点恢复。如果代理未执行该提交，其消息会被重新投递；如果代理已执行提交、只是响应丢失，则不会重新投递。
- 连接的异常监听器收到连接丢失通知时，读取器失败。
- 并行读取器是队列的竞争消费者。检查点状态只保存逻辑消费者分片。
- 空队列不表示输入结束；作业一直运行到被取消。

## 提供方说明

- **ActiveMQ Classic 预取。** 默认 `queuePrefetch` 为 1000 时，已封存的消费者会保留其预取的消息，直到其会话被提交，因此这些消息要等到下一个检查点，并且可能在较晚的消息之后被读取。请设置较低的预取值，例如在 `connection_factory_properties` 中设置 `prefetchPolicy.queuePrefetch = 0`。ActiveMQ Artemis 和 Qpid JMS 在关闭已封存会话的消费者时会归还预取的消息。
- **IBM MQ。** 请使 `max_in_flight_messages` 低于队列管理器的 `MaxUncommittedMsgs`。

## 示例

ActiveMQ Artemis：

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  Jms {
    connection_factory_class = "org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory"
    connection_factory_properties {
      brokerURL = "tcp://artemis-host:61616"
    }
    username = "seatunnel"
    password = "******"
    destination = "orders"
    schema {
      fields {
        id = bigint
        name = string
      }
    }
  }
}

sink {
  Console {}
}
```

使用较低预取值的 ActiveMQ Classic：

```hocon
source {
  Jms {
    connection_factory_class = "org.apache.activemq.ActiveMQConnectionFactory"
    connection_factory_properties {
      brokerURL = "tcp://activemq-host:61616"
      prefetchPolicy.queuePrefetch = 0
    }
    destination = "orders"
    schema {
      fields {
        id = bigint
        name = string
      }
    }
  }
}
```

通过 Qpid JMS 访问 AMQP 1.0 代理，使用选择器和分隔文本：

```hocon
source {
  Jms {
    connection_factory_class = "org.apache.qpid.jms.JmsConnectionFactory"
    connection_factory_properties {
      remoteURI = "amqp://amqp-host:5672"
    }
    destination = "orders"
    message_selector = "region = 'eu'"
    format = TEXT
    field_delimiter = "|"
    schema {
      fields {
        id = bigint
        name = string
      }
    }
  }
}
```

从 JNDI 获取连接工厂，这里使用 ActiveMQ Artemis 的 JNDI 提供方：

```hocon
source {
  Jms {
    jndi_initial_context_factory = "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory"
    jndi_connection_factory_name = "SeaTunnelFactory"
    jndi_properties {
      connectionFactory.SeaTunnelFactory = "tcp://artemis-host:61616"
    }
    destination = "orders"
    schema {
      fields {
        id = bigint
        name = string
      }
    }
  }
}
```

## 变更日志

<ChangeLog />
