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

## 描述

用于将 SeaTunnel 行数据发送到 Slack 频道。流处理和批处理作业都支持。

> 连接器会把一行中的字段值拼成一条用逗号分隔的 Slack 消息。例如，字段值为 `huan` 和 `17` 时，
> 发送内容为 `huan,17`。

## 数据类型映射

所有字段值在发送到 Slack 前都会转换为字符串。

## 选项

| 名称           | 类型   | 必需 | 默认值 | 描述 |
|----------------|--------|------|--------|------|
| webhooks_url   | String | 是   | -      | Slack webhook URL。 |
| oauth_token    | String | 是   | -      | 用于列出频道并发送消息的 Slack OAuth 令牌。 |
| slack_channel  | String | 是   | -      | 写入数据的 Slack 频道名称。 |
| common-options |        | 否   | -      | 接收器插件通用参数，详见 [Sink 常见选项](../common-options/sink-common-options.md)。 |

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

## 变更日志

<ChangeLog />
