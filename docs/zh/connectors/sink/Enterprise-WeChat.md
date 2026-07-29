import ChangeLog from '../changelog/connector-http-wechat.md';

# Enterprise WeChat

> Enterprise WeChat 接收器连接器

## 描述

一个将 SeaTunnel 行数据发送到企业微信机器人 webhook 的接收插件。作业配置中的连接器标识符是 `WeChat`。

> 例如，如果来自上游的数据是 [`"alarmStatus": "firing", "alarmTime": "2022-08-03 01:38:49"，"alarmContent": "The disk usage exceeds the threshold"`], 微信机器人的输出内容如下:
>
> ```
> alarmStatus: firing 
> alarmTime: 2022-08-03 01:38:49
> alarmContent: The disk usage exceeds the threshold
> ```
>
> **提示：WeChat 接收器发送文本消息。每一行数据会先格式化为 `字段名: 字段值` 的多行文本，再发送到 webhook。**

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称                  | 类型   | 必需 | 默认值 | 描述 |
|-----------------------|--------|------|--------|------|
| url                   | String | 是   | -      | 企业微信机器人 webhook URL。 |
| mentioned_list        | array  | 否   | -      | 需要提醒的用户 ID 列表，使用 `@all` 提醒所有人。 |
| mentioned_mobile_list | array  | 否   | -      | 需要提醒的手机号列表，使用 `@all` 提醒所有人。 |
| retry                 | int    | 否   | -      | HTTP 请求抛出 `IOException` 时的最大重试次数。 |
| retry_backoff_multiplier_ms | int | 否 | 100    | 重试退避倍数，单位毫秒。 |
| retry_backoff_max_ms  | int    | 否   | 10000  | 最大重试退避时间，单位毫秒。 |
| multi_table_sink_replica | int | 否   | -      | 多表写入时使用的 sink 副本数。 |
| common-options        |        | 否   | -      | 接收器插件通用参数。 |

### url [string]

企业微信 webhook URL 格式为 `https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=XXXXXX`。

### mentioned_list [array]

需要提醒的用户 ID 列表，使用 `@all` 提醒所有人。如果无法获取用户 ID，可以使用 `mentioned_mobile_list`。

### mentioned_mobile_list [array]

需要提醒的手机号列表，使用 `@all` 提醒所有人。

### common options

接收器插件常用参数，详见 [Sink Common Options](../common-options/sink-common-options.md) 

## 示例

简单的例子:

```hocon
WeChat {
  url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=693axxx6-7aoc-4bc4-97a0-0ec2sifa5aaa"
}
```

```hocon
WeChat {
  url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=693axxx6-7aoc-4bc4-97a0-0ec2sifa5aaa"
  mentioned_list = ["wangqing", "@all"]
  mentioned_mobile_list = ["13800001111", "@all"]
}
```

## 变更日志

<ChangeLog />
