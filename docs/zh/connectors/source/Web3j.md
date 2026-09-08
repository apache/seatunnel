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

该连接器只使用单个分片，不支持并行度。每一条数据对应一次 `eth_blockNumber` HTTP 调用，
实际轮询节奏由所配置的 Provider 响应速度决定。

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

## 注意事项

- `url` 必须指向兼容 JSON-RPC 的 Web3 Provider，例如 Infura、Alchemy 或者自建的以太坊节点。
  推荐使用 HTTPS；连接器不再做额外的鉴权，如果 Provider 需要 API Key，直接把 Key 写在 URL
  里即可。
- 连接器只暴露包含 `value` 字段的固定行结构。如需进一步处理 `blockNumber` 或 `timestamp`，
  请在下游使用 SQL Transform 或 JSON Path。
- 流处理模式下，连接器会保持 HTTP 连接持续打开，并按轮询节奏把观察到的最新区块号写入下游；
  只有当下游 Sink 需要 checkpoint 时才建议配置 `checkpoint.interval`。

## 示例

批处理模式下，Source 输出一行数据后即结束：

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

流处理模式下，连接器持续轮询 Provider，每次轮询都会输出一行包含当前最新区块号的记录：

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  Web3j {
    url = "https://mainnet.infura.io/v3/xxxxx"
    plugin_output = "web3j"
  }
}

sink {
  Assert {
    plugin_input = "web3j"
    rules {
      field_rules = [
        {
          field_name = value
          field_type = string
          field_value = [
            {
              rule_type = NOT_NULL
            }
          ]
        }
      ]
    }
  }
}
```

## 常见问题

### 轮询节奏是怎么控制的？

连接器每次轮询都会发一次 HTTP `eth_blockNumber` RPC，等到 provider 返回结果后再把这一条数据发出来。因此实际的发数据节奏取决于所配置 provider 的响应速度——无法用一个独立的配置项直接设定。如果需要可重放的背压，请让 Source 配合一个会做 checkpoint 的下游 Sink；Web3j Source 自身没有限流或节流配置。

### `value` 字段里到底装了什么？

一个结构固定的 JSON 对象：`{"blockNumber": <number>, "timestamp": "<ISO-8601 UTC>"}`。`blockNumber` 是 provider 最新返回的区块号，`timestamp` 是连接器在拿到响应那一刻生成的时间戳。连接器不会解析 `value` 的内容；如果还需要交易数、Gas、节点元信息等更多字段，请改用其它 RPC 或在下游通过 SQL Transform 处理。

### Web3j Source 除了 URL 之外还支持别的鉴权方式吗？

不支持。连接器只会把配置的 `url` 原样传给底层的 HTTP 客户端。所以鉴权也只能是 URL 里能承载的那种——通常是 hosted 服务（Infura / Alchemy 等）直接嵌在 URL 里的 API Key。连接器没有基于 Header 的鉴权路径，请不要把需要频繁轮换的密钥写在 URL 里；直接使用一个长期的 provider Key，并通过你们平时的密钥管理系统托管。

## 变更日志

<ChangeLog />

