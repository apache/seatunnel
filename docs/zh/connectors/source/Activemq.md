import ChangeLog from '../changelog/connector-activemq.md';

# ActiveMQ

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

> ActiveMQ Classic 队列数据源连接器

## 描述

通过 OpenWire 从一个 ActiveMQ Classic 队列读取 JMS `TextMessage`，复用现有 ActiveMQ 模块以及 JSON 和文本反序列化器。本连接器不是通用 JMS 或 AMQP 实现。

## 支持的引擎

> Flink<br/>
> SeaTunnel Zeta<br/>

数据源要求流模式和周期性检查点。本次不支持 Spark：尚未验证其虚拟数据源检查点是否能作为删除队列消息所需的端到端提交边界。

## 主要特性

- [ ] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [自定义分片](../../introduction/concepts/connector-v2-features.md)

## 配置项

| 名称 | 类型 | 必填 | 默认值 |
| --- | --- | --- | --- |
| uri | string | 是 | - |
| queue_name | string | 是 | - |
| username | string | 否 | - |
| password | string | 否 | - |
| format | enum | 否 | JSON |
| field_delimiter | string | 否 | , |
| max_in_flight_messages | int | 否 | 1000 |
| schema | config | 是 | - |

### uri [string]

单个代理地址，格式为 `tcp://host:port` 或 `ssl://host:port`。不支持 URI 中的凭据、查询参数、路径或包括 `failover:` 在内的复合传输。凭据必须使用单独的配置项。TLS 连接应在每个工作节点配置 JVM 信任库和密钥库并使用 `ssl://`，不要禁用证书校验。明文 TCP 仅应在可信网络内使用。

连接中断时任务失败，由引擎恢复读取器并创建新连接，不在原会话内重连或复用旧确认句柄。

### queue_name [string]

一个普通队列名称，不支持通配符、复合队列、目标前缀或 `?consumer.*` 参数。本次不支持主题、持久主题订阅和选择器。应提前创建队列并配置账号权限；连接器不控制代理的自动创建策略。

### username / password [string]

可选凭据，必须同时配置。省略时客户端不提供显式凭据。账号权限应限定到目标队列，不要在 URI 中嵌入凭据。

### format / field_delimiter

`JSON` 按 `schema` 映射对象或对象数组；`TEXT` 使用现有分隔文本反序列化器，分隔符 `field_delimiter` 默认为 `,`。仅支持 `TextMessage`；对象消息、字节消息、null 或空字符串消息体以及无效内容使任务失败且不确认该消息，不进行 Java 对象反序列化。反序列化或输出失败时不保留可能包含消息内容的原始异常详情；请通过有权限的代理客户端检查消息与 schema、format 是否匹配。

### max_in_flight_messages [int]

每个读取器允许保留的已输出但尚未确认的消息数，必须大于零。达到上限后暂停接收，完成检查点后恢复。客户端队列预取数设置为相同值，预取消息可能额外占用内存。这是数量限制而不是字节限制，应按工作节点内存和生产者消息大小配置。持续吞吐依赖定期成功完成的检查点。

### schema [config]

消息内容的模式，参见[模式特性](../../introduction/concepts/schema-feature.md)。本次不将消息头或属性暴露为元数据。

### 公共配置

参见[数据源公共配置](../common-options/source-common-options.md)。

## 交付与恢复

- 语义为**至少一次**，不是精确一次。只有包含已输出行的检查点完成后，才逐条确认消息。后续成功检查点也可覆盖之前中止的检查点中的消息。
- 确认前失败会关闭连接，未确认消息可由代理重新投递。恢复可能产生重复行，包括部分输出的多行 JSON 消息；需要时使用幂等下游。
- 检查点只保存逻辑消费者分片，不保存 JMS 消息对象或队列偏移量；代理负责保留和重新投递未确认消息。
- 每个活跃并行读取器拥有独立连接、会话和竞争消费者。初次启动每个读取器分配一个分片；恢复保留已有逻辑分片，增加并行度时补充新分片。缩容后一个读取器可以持有多个恢复分片，但只使用一个消费者。并行消费和重投递不保证全局顺序。
- 空队列不表示输入结束，读取器一直等待到作业取消或失败。
- 保留和重投递受代理持久化、消息交付模式、过期时间及死信策略影响。要跨代理重启保留消息，需要持久代理及持久消息。永久无效消息可能重复导致任务失败或按代理策略进入死信队列；连接器不会静默跳过。

## 示例

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  ActiveMQ {
    uri = "tcp://activemq-host:61616"
    queue_name = "events"
    format = JSON
    max_in_flight_messages = 1000
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

对于 `42|created` 等分隔文本，配置 `format = TEXT`、`field_delimiter = "|"`，并按字段顺序定义模式。现有 ActiveMQ Sink 的配置和行为保持不变；上述 Source 配置不适用于 Sink。

## 变更日志

<ChangeLog />
