import ChangeLog from '../changelog/connector-socket.md';

# Socket

> Socket 源连接器

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [x] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

## 描述

用于从 Socket 服务端读取按行分隔的文本数据。Socket 中收到的每一行都会成为一条 `STRING` 类型的
SeaTunnel 数据。流处理模式下连接器保持连接持续打开并按行处理；批处理模式下读取器只执行一次 `read`，
将这次读取中已按 `\n` 切分得到的完整行（以及最后一行末尾不完整的部分作为一行）发送出去后即结束——
它既不会等待对端关闭连接，也没有读取超时设置。

该连接器只使用单个 split（Source 并行度固定为 1）。`host`/`port` 指的是 SeaTunnel 要连接的
*服务端* 地址，对端可以是 Sink、Transform，也可以通过 `nc -l` 等工具手动提供。

## 数据类型映射

Socket Source 会把每一行输入读取为字符串。

| SeaTunnel 数据类型 |
|------------------|
| STRING |

## 选项

| 参数名 | 类型 | 必须 | 默认值 | 描述 |
|--------|------|------|--------|------|
| host | String | 是 | - | socket 服务器主机 |
| port | Integer | 是 | - | socket 服务器端口 |
| common-options | | 否 | - | 源插件通用参数，请参考 [源通用选项](../common-options/source-common-options.md) 详见。 |

:::tip

Socket Source 更适合本地调试和简单文本流读取。它不会保存 Socket 服务端的读取位点，如果需要可重放或精确一次读取，请使用 Kafka 等具备位点管理能力的 Source。每行都会作为一条数据
处理；空行不会被跳过，而是会产生一个负载为空字符串的行。

:::

## 如何创建 Socket 数据同步作业

* 配置 SeaTunnel 配置文件

以下示例演示如何创建从 Socket 读取数据并在本地客户端上打印的数据同步作业：

```bash
# 设置要执行的任务的基本配置
env {
  parallelism = 1
  job.mode = "BATCH"
}

# 创建源以连接到 socket
source {
    Socket {
        host = "localhost"
        port = 9999
    }
}

# 控制台打印读取的 socket 数据
sink {
  Console {
    parallelism = 1
  }
}
```

* 启动端口监听

```shell
nc -l 9999
```

* 启动 SeaTunnel 任务

* Socket 源发送测试数据

```text
~ nc -l 9999
test
hello
flink
spark
```

* 控制台 Sink 打印数据

```text
[test]
[hello]
[flink]
[spark]
```

### 流处理模式

流处理模式下，源端会保持连接持续打开，持续读取新行。建议配合可以缓冲或 checkpoint 的下游 Sink：

```bash
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  Socket {
    host = "localhost"
    port = 9999
  }
}

sink {
  Console {
    parallelism = 1
  }
}
```

## 常见问题

### Socket Source 会对读取位点做 checkpoint 吗？

不会。连接器不会记录服务端偏移量或消息序号，重启后会从 socket 当前能读到的内容继续读下一次。如果任务需要可重放或精确一次，请改用 Kafka、Pulsar 等具备位点跟踪能力的 Source。Socket Source 主要面向本地调试或一次性文本流。

### 空行和末尾不完整的记录会怎样处理？

批处理模式下，读取器把 socket 上当前缓冲的内容一次性读出来，按 `\n` 切分，每条完整的行作为一条 STRING 数据发出去；如果末尾还有一段不完整的行，也会作为最后一行发出再结束任务。空行*不会*被跳过——它会变成一条内容为空字符串的数据。流处理模式下空行也是同样按原样即时发出去。

### Socket Source 能不能并行？

不能。读取器只会和配置的 `host:port` 建立一条客户端连接，使用单个 split，因此该 Source 在任务里的 `env.parallelism` 实际上被限制为 1。要提高吞吐，请改跑多个独立任务（每个任务使用各自的 `host`/`port`），而不是在同一个 Socket Source 里调高并行度。

## 变更日志

<ChangeLog />

