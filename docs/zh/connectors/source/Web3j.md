import ChangeLog from '../changelog/connector-web3j.md';

# Web3j

> Web3j 源连接器

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [x] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行性](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

## 描述

Web3j 的源连接器。用于从区块链读取数据，例如区块信息、交易、智能合约事件等。目前支持读取区块高度数据。

## 源选项

| 参数名 | 类型 | 必须 | 默认值 | 描述 |
|--------|------|------|--------|------|
| url | String | 是 | - | 使用 Infura 作为服务提供商时，URL 用于与以太坊网络通信。 |

## 如何创建 Http 数据同步作业

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Web3j {
    url = "https://mainnet.infura.io/v3/xxxxx"
  }
}

# 控制台打印读取的 Http 数据
sink {
  Console {
    parallelism = 1
  }
}
```

然后您将获得以下数据：

```json
{"blockNumber":19525949,"timestamp":"2024-03-27T13:28:45.605Z"}
```

## 变更日志

<ChangeLog />

