import ChangeLog from '../changelog/connector-web3j.md';

# Web3j

> Web3j source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> Seatunnel Zeta<br/>

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Description

Source connector for web3j. It is used to read data from the blockchain, such as block information, transactions, smart contract events, etc.  Currently, it supports reading block height data.

## Source Options

| Name |  Type  | Required | Default |                                               Description                                               |
|------|--------|----------|---------|---------------------------------------------------------------------------------------------------------|
| url  | String | Yes      | -       | When using Infura as the service provider, the URL is used for communication with the Ethereum network. |

## How to Create a Http Data Synchronization Jobs

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

# Console printing of the read Http data
sink {
  Console {
    parallelism = 1
  }
}
```

Then you will get the following data:

```json
{"blockNumber":19525949,"timestamp":"2024-03-27T13:28:45.605Z"}
```

## Changelog

<ChangeLog />
