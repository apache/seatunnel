import ChangeLog from '../changelog/connector-slack.md';

# Slack

> Slack 接收器连接器

## 支持以下引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

## 描述

用于将 SeaTunnel 行数据发送到 Slack 频道，支持流处理和批处理作业。连接器首先使用配置的 OAuth 令牌
查找频道 ID，然后通过 Slack Web API 将每一行以逗号分隔的消息发布到该频道。

## 数据类型映射

Slack 连接器会把一行中的每个字段通过 `String.valueOf(value)` 转为字符串，再用逗号拼接成一条纯文本
消息 —— 线上传输的是单一文本消息，不存在按字段区分的 JSON 结构，因此连接器可以发布任意类型的
SeaTunnel 行。

## 选项

|       名称       |  类型  | 必需 | 默认值 | 描述                                                                                              |
|------------------|--------|------|--------|---------------------------------------------------------------------------------------------------|
| webhooks_url     | String | 是   | -      | Slack 传入 Webhook URL，连接器在初始化时会校验该选项；消息发送路径使用 `oauth_token`、`slack_channel` 通过 Slack Web API 发布消息。 |
| oauth_token      | String | 是   | -      | 用于查询频道和发送消息的 Slack OAuth 令牌。                                                       |
| slack_channel    | String | 是   | -      | 行数据发送到的 Slack 频道名称，连接器会通过 OAuth 令牌将其解析为频道 ID。                          |
| common-options   |        | 否   | -      | 接收器插件通用参数，详见 [Sink 常见选项](../common-options/sink-common-options.md)。              |

### webhooks_url [String]

目标 Slack 工作空间中配置的传入 Webhook URL。连接器在初始化时会校验该选项；消息发送路径使用
`oauth_token` 和 `slack_channel` 配合 Slack Web API 来解析频道 ID 并发布消息。

### oauth_token [String]

至少需要 `chat:write` 和 `channels:read`（或同等）权限的 Slack OAuth 令牌。该令牌用于调用
`conversations.list` 和 `chat.postMessage` 接口。

### slack_channel [String]

行数据要发送到的 Slack 频道名称。连接器会通过 Slack Web API 将频道名解析为频道 ID。OAuth 令牌
必须能访问该频道。

### common options

接收器插件通用参数，请参考 [Sink 常见选项](../common-options/sink-common-options.md) 了解详情。

## 任务示例

### 简单示例

```hocon
sink {
  Slack {
    webhooks_url = "https://hooks.slack.com/services/xxxxxxxxxxxx/xxxxxxxxxxxx/xxxxxxxxxxxxxxxx"
    oauth_token = "xoxp-xxxxxxxxxx-xxxxxxxx-xxxxxxxxx-xxxxxxxxxxx"
    slack_channel = "seatunnel-alerts"
  }
}
```

### 配合上游源使用

将 fake 源产生的行数据转发到 Slack 的简单批处理作业。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        user = string
        age = int
      }
    }
    rows = [
      { kind = "INSERT", fields = ["huan", 17] }
    ]
  }
}

sink {
  Slack {
    webhooks_url = "https://hooks.slack.com/services/xxxxxxxxxxxx/xxxxxxxxxxxx/xxxxxxxxxxxxxxxx"
    oauth_token = "xoxp-xxxxxxxxxx-xxxxxxxx-xxxxxxxxx-xxxxxxxxxxx"
    slack_channel = "seatunnel-alerts"
  }
}
```

连接器会把一行中的字段值拼成一条用逗号分隔的 Slack 消息，因此上面的示例会在配置的频道中产生
`huan,17` 这条消息。

## 变更日志

<ChangeLog />
