import ChangeLog from '../changelog/connector-dingtalk.md';

# 钉钉

> 钉钉数据接收器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

## 描述

通过钉钉自定义机器人 Webhook，将 SeaTunnel 行数据发送到钉钉群聊的接收器插件。作业配置中使用的连接器标识为 `DingTalk`。每一行数据都会使用配置的机器人密钥进行签名，然后发送到钉钉机器人地址。

## 数据类型映射

钉钉连接器会把每一行通过 `SeaTunnelRow.toString()` 序列化为纯文本，并作为一条消息发送给钉钉机器人。
线上传输的是单一文本消息，不存在按字段区分的 JSON 结构 —— 不论源字段类型是什么，整行都会被转换为
一条文本消息。

## 接收器选项

|     名称      |  类型  | 是否必须 | 默认值 | 描述                                                                                          |
|---------------|--------|----------|--------|-----------------------------------------------------------------------------------------------|
| url           | String | 是       | -      | 钉钉机器人 Webhook 地址，格式 `https://oapi.dingtalk.com/robot/send?access_token=XXXXXX`。   |
| secret        | String | 是       | -      | 用于对请求进行签名的钉钉机器人密钥。                                                          |
| common-options|        | 否       | -      | Sink 插件通用参数，详见 [Sink 常见选项](../common-options/sink-common-options.md)。           |

### url [String]

钉钉机器人地址格式为 `https://oapi.dingtalk.com/robot/send?access_token=XXXXXX`，其中 `access_token`
是钉钉群机器人设置中生成的令牌。

### secret [String]

钉钉机器人密钥，用于对发往 `url` 中机器人的消息进行签名。连接器使用该密钥为消息生成签名，以便
钉钉端校验请求来源。该密钥必须与 `url` 中机器人绑定的密钥保持一致。签名客户端在写入器首次发送时
按需创建一次，并在该写入器生命周期内复用，不会对每条消息重新计算签名。

### common options

Sink 插件通用参数，请参考 [Sink 常见选项](../common-options/sink-common-options.md) 了解详情。

## 任务示例

### 简单示例

通过已配置的机器人将行数据发送到钉钉群。

```hocon
sink {
  DingTalk {
    url = "https://oapi.dingtalk.com/robot/send?access_token=ec646cccd028d978a7156ceeac5b625ebd94f586ea0743fa501c100007890"
    secret = "SEC093249eef7aa57d4388aa635f678930c63db3d28b2829d5b2903fc1e5c10000"
  }
}
```

### 配合上游源使用

一个典型的端到端作业，从 fake 源读取数据并转发到钉钉。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        id = int
        name = string
        score = double
      }
    }
    rows = [
      { kind = "INSERT", fields = [1, "alice", 9.5] }
    ]
  }
}

sink {
  DingTalk {
    url = "https://oapi.dingtalk.com/robot/send?access_token=ec646cccd028d978a7156ceeac5b625ebd94f586ea0743fa501c100007890"
    secret = "SEC093249eef7aa57d4388aa635f678930c63db3d28b2829d5b2903fc1e5c10000"
  }
}
```

## 变更日志

<ChangeLog />
