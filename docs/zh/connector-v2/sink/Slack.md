# Slack

> Slack 接收器连接器

## 支持以下引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [ ] [精确一次](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

## 描述

用于将数据发送到Slack Channel.两者都支持流媒体和批处理模式.

> 例如，如果来自上游的数据是 [`age: 12, name: huan`], 则发送到套接字服务器的内容如下: `{"name":"huan","age":17}`

## 数据类型映射

所有数据类型都映射到字符串.

## 选项

|      名称                 |  类型   | 必需 | 默认值 | 描述                                                             |
|----------------|--------|----------|---------|----------------------------------------------------------------|
| webhooks_url   | String | Yes      | -       | Slack webhook 的 url                                            |
| oauth_token    | String | Yes      | -       | 用于实际身份验证的Slack oauth令牌                                         |
| slack_channel  | String | Yes      | -       | 用于数据写入的slack channel                                           |
| common-options |        | no       | -       | 接收器插件常用参数, 详见 [Sink 常见选项](../sink-common-options.md) |

## 任务示例

### 简单示例:

```hocon
sink {
 SlackSink {
  webhooks_url = "https://hooks.slack.com/services/xxxxxxxxxxxx/xxxxxxxxxxxx/xxxxxxxxxxxxxxxx"
  oauth_token = "xoxp-xxxxxxxxxx-xxxxxxxx-xxxxxxxxx-xxxxxxxxxxx"
  slack_channel = "channel name"
 }
}
```

## 变更日志

### 新版本

- 添加 Slack 接收器连接器

