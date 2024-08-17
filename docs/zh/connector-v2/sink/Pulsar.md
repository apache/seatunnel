# Pulsar

> Pulsar 数据连接器

## 引擎支持

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 核心特性

- [x] [精准一次](../../concept/connector-v2-features.md)

## 描述

Apache Pulsar 的接收连接器。

## 支持的数据源信息

|  数据源   |   支持的版本   |
|--------|-----------|
| Pulsar | Universal |

## 输出选项

|          名称          |   类型   | 是否必须 |         默认值         |                       描述                        |
|----------------------|--------|------|---------------------|-------------------------------------------------|
| topic                | String | Yes  | -                   | 输出到Pulsar主题名称.                                  |
| client.service-url   | String | Yes  | -                   | Pulsar 服务的服务 URL 提供者.                           |
| admin.service-url    | String | Yes  | -                   | 管理端点的 Pulsar 服务 HTTP URL.                       |
| auth.plugin-class    | String | No   | -                   | 身份验证插件的名称.                                      |
| auth.params          | String | No   | -                   | 身份验证插件的参数.                                      |
| format               | String | No   | json                | 数据格式。默认格式为 json。可选的文本格式.                        |
| field_delimiter      | String | No   | ,                   | 自定义数据格式的字段分隔符.                                  |
| semantics            | Enum   | No   | AT_LEAST_ONCE       | 写入 pulsar 的一致性语义.                               |
| transaction_timeout  | Int    | No   | 600                 | 默认情况下，事务超时指定为 10 分钟.                            |
| pulsar.config        | Map    | No   | -                   | 除了上述必须由 Pulsar 生产者客户端指定的参数外.                    |
| message.routing.mode | Enum   | No   | RoundRobinPartition | 要分区的消息的默认路由模式.                                  |
| partition_key_fields | array  | No   | -                   | 配置哪些字段用作 pulsar 消息的键.                           |
| common-options       | config | no   | -                   | 源插件常用参数，详见源码 [常用选项](../sink-common-options.md). |

## 参数解释

### client.service-url [String]

Pulsar 服务的 Service URL 提供程序。要使用客户端库连接到 Pulsar，
您需要指定一个 Pulsar 协议 URL。您可以将 Pulsar 协议 URL 分配给特定集群并使用 Pulsar 方案。

例如, `localhost`: `pulsar://localhost:6650,localhost:6651`.

### admin.service-url [String]

管理端点的 Pulsar 服务 HTTP URL.

例如, `http://my-broker.example.com:8080`, or `https://my-broker.example.com:8443` for TLS.

### auth.plugin-class [String]

身份验证插件的名称。

### auth.params [String]

身份验证插件的参数。

例如, `key1:val1,key2:val2`

### format [String]

数据格式。默认格式为 json。可选的文本格式。默认字段分隔符为","。如果自定义分隔符，请添加"field_delimiter"选项。

### field_delimiter [String]

自定义数据格式的字段分隔符。默认field_delimiter为','。

### semantics [Enum]

写入 pulsar 的一致性语义。可用选项包括 EXACTLY_ONCE、NON、AT_LEAST_ONCE、默认AT_LEAST_ONCE。
如果语义被指定为 EXACTLY_ONCE，我们将使用 2pc 来保证消息被准确地发送到 pulsar 一次。
如果语义指定为 NON，我们将直接将消息发送到 pulsar，如果作业重启/重试或网络错误，数据可能会重复/丢失。

### transaction_timeout [Int]

默认情况下，事务超时指定为 10 分钟。如果事务未在指定的超时时间内提交，则事务将自动中止。因此，您需要确保超时大于检查点间隔。

### pulsar.config [Map]

除了上述 Pulsar 生产者客户端必须指定的参数外，用户还可以为生产者客户端指定多个非强制性参数，
涵盖 Pulsar 官方文档中指定的所有生产者参数。

### message.routing.mode [Enum]

要分区的消息的默认路由模式。可用选项包括 SinglePartition、RoundRobinPartition。
如果选择 SinglePartition，如果未提供密钥，分区生产者将随机选择一个分区并将所有消息发布到该分区中，如果消息上提供了密钥，则分区生产者将对密钥进行哈希处理并将消息分配给特定分区。
如果选择 RoundRobinPartition，则如果未提供密钥，则生产者将以循环方式跨所有分区发布消息，以实现最大吞吐量。请注意，轮询不是按单个消息完成的，而是设置为相同的批处理延迟边界，以确保批处理有效。

### partition_key_fields [String]

配置哪些字段用作 pulsar 消息的键。

例如，如果要使用上游数据中的字段值作为键，则可以为此属性分配字段名称。

上游数据如下：

| name | age |     data      |
|------|-----|---------------|
| Jack | 16  | data-example1 |
| Mary | 23  | data-example2 |

如果将 name 设置为键，则 name 列的哈希值将确定消息发送到哪个分区。

如果未设置分区键字段，则将向 null 消息键发送至。

消息键的格式为 json，如果 name 设置为键，例如 '{“name”：“Jack”}'。

所选字段必须是上游的现有字段。

### 常见选项

源插件常用参数，详见源码[常用选项](../sink-common-options.md) .

## 任务示例

### 简单:

> 该示例定义了一个 SeaTunnel 同步任务，该任务通过 FakeSource 自动生成数据并将其发送到 Pulsar Sink。FakeSource 总共生成 16 行数据 （row.num=16），每行有两个字段，name（字符串类型）和 age（int 类型）。最终目标主题是test_topic主题中还将有 16 行数据。 如果您尚未安装和部署 SeaTunnel，则需要按照[安装Seatunnel](../../start-v2/locally/deployment.md) SeaTunnel 中的说明安装和部署 SeaTunnel。然后按照 [SeaTunnel 引擎快速入门](../../start-v2/locally/quick-start-seatunnel-engine.md)中的说明运行此作业。

```hocon
# Defining the runtime environment
env {
  # You can set flink configuration here
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    parallelism = 1
    result_table_name = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Pulsar {
  	topic = "example"
    client.service-url = "localhost:pulsar://localhost:6650"
    admin.service-url = "http://my-broker.example.com:8080"
    result_table_name = "test"
    pulsar.config = {
        sendTimeoutMs = 30000
    }
  }
}
```

## 更改日志

### 下一个版本

- 添加 Pulsar Sink 连接器

