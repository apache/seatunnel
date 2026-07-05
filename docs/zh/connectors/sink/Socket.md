import ChangeLog from '../changelog/connector-socket.md';

# Socket

> Socket 数据接收器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 描述

用于向 Socket Server 发送数据，支持流模式和批模式。每条 SeaTunnel 数据会被序列化为一行 JSON。

> 例如，如果来自上游的数据是 [`age: 17, name: jared`]，则发送到 Socket Server 的内容如下：`{"name":"jared","age":17}`

## Sink 选项

|      名称      |  类型   | 是否必传 | 默认值  |                                                   描述                                                   |
|----------------|---------|----------|---------|-----------------------------------------------------------------------------------------------------------------|
| host           | String  | 是      | -       | socket 服务器主机                                                                                              |
| port           | Integer | 是      | -       | socket 服务器端口                                                                                              |
| max_retries    | Integer | 否       | 3       | 发送失败后的最大重试次数                                                                     |
| common-options |         | 否       | -       | Sink 插件通用参数，详见 [Sink 通用选项](../common-options/sink-common-options.md) |

:::tip

Socket Sink 更适合本地调试和简单集成。它会根据 `max_retries` 进行重连和重试，但不提供精确一次写入保证。

:::

## 任务示例

> 以下示例把 FakeSource 随机生成的数据写入 Socket Server。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  Socket {
    host = "localhost"
    port = 9999
  }
}
```

* 启动端口侦听

```shell
nc -l -v 9999
```

* 启动 SeaTunnel 任务

* Socket 服务器控制台打印数据

```text
{"name":"jared","age":17}
```

## 变更日志

<ChangeLog />
