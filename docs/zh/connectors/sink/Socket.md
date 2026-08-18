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

用于向 Socket Server 发送数据，支持流模式和批模式。每条 SeaTunnel 数据会被 `JsonSerializationSchema`
序列化为一个 JSON 对象，并写入配置的 TCP 端口。**连接器不会追加任何分隔符**——既不会追加换行符，也不会
在记录之间追加任何其它分隔符。因此多条记录会作为一条无分隔、连续的 TCP 字节流直接拼接在一起传输
（例如 `{"a":1}{"a":2}{"a":3}`）。输出明确*不是*按行分隔的 JSON，因此对端需要自行处理分帧：
使用支持连续读取多个 JSON 值的流式解析器（例如 Jackson 的 `MappingIterator`），而不是按行解析的解析器。
`nc -l` 这类工具只会原样回显拼接后的字节，适合做单条记录的快速验证，但无法自行切分多条记录。

> 例如，如果来自上游的数据是 [`age: 17, name: jared`]，则发送到 Socket Server 的内容如下：`{"name":"jared","age":17}`

## Sink 选项

|      名称      |  类型   | 是否必传 | 默认值  |                                                   描述                                                   |
|----------------|---------|----------|---------|----------------------------------------------------------------------------------------------------------------|
| host           | String  | 是      | -       | socket 服务器主机                                                                                              |
| port           | Integer | 是      | -       | socket 服务器端口                                                                                              |
| max_retries    | Integer | 否       | 3       | 发送失败后的最大重试次数。设置为 `-1` 表示无限重试，`0` 表示失败后立即抛出异常。                              |
| common-options |         | 否       | -       | Sink 插件通用参数，详见 [Sink 通用选项](../common-options/sink-common-options.md) |

:::tip

Socket Sink 更适合本地调试和简单集成。它会根据 `max_retries` 进行重连和重试，但不提供精确一次写入保证。
每个 Writer 会建立一条 TCP 连接；`host`/`port` 指的是客户端要连接的 *服务端* 地址。

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
    max_retries = 3
  }
}
```

* 启动端口侦听

```shell
nc -l -v 9999
```

* 启动 SeaTunnel 任务

* Socket 服务器控制台打印数据。由于不会追加分隔符，多条记录在原始字节流中以拼接的 JSON 对象形式到达（下面的换行仅为便于阅读）：

```text
{"name":"jared","age":17}{"name":"jared","age":18}...
```

## 变更日志

<ChangeLog />
