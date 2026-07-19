import ChangeLog from '../changelog/connector-web3j.md';

# Web3j

> Web3j 源连接器

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 描述

Web3j 源连接器用于通过 Web3 服务端点读取区块链数据。目前连接器读取最新区块号，并输出一个
`value` 字段。`value` 字段是 JSON 字符串，里面包含 `blockNumber` 和连接器生成的读取时间戳。

批处理模式下，source 输出一行后结束。流处理模式下，它会持续轮询服务端点，并输出观察到的最新区块号。

## 源选项

| 参数名 | 类型 | 必须 | 默认值 | 描述 |
|--------|------|------|--------|------|
| url | String | 是 | - | 用于和以太坊网络通信的 Web3 服务端点，例如 Infura URL。 |

## 输出字段

| 字段 | 类型 | 描述 |
|------|------|------|
| value | String | JSON 字符串，包含最新区块号和连接器生成的时间戳。 |

`value` 中保存的 JSON 结构如下：

```json
{"blockNumber":19525949,"timestamp":"2024-03-27T13:28:45.605Z"}
```

## 示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Web3j {
    url = "https://mainnet.infura.io/v3/xxxxx"
    plugin_output = "web3j"
  }
}

sink {
  Console {
    plugin_input = "web3j"
    parallelism = 1
  }
}
```

然后可以得到类似下面的数据：

```json
{"value":"{\"blockNumber\":19525949,\"timestamp\":\"2024-03-27T13:28:45.605Z\"}"}
```

## 变更日志

<ChangeLog />
