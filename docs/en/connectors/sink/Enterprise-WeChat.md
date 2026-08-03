import ChangeLog from '../changelog/connector-http-wechat.md';

# Enterprise WeChat

> Enterprise WeChat sink connector

## Description

A sink plugin that sends SeaTunnel rows to an Enterprise WeChat robot webhook. The connector identifier in job
configuration is `WeChat`.

> For example, if the data from upstream is [`"alarmStatus": "firing", "alarmTime": "2022-08-03 01:38:49"，"alarmContent": "The disk usage exceeds the threshold"`], the output content to WeChat Robot is the following:
>
> ```
> alarmStatus: firing 
> alarmTime: 2022-08-03 01:38:49
> alarmContent: The disk usage exceeds the threshold
> ```
>
> **Tips: The WeChat sink sends text messages. Each row is formatted as `fieldName: fieldValue` lines before it is sent to the webhook.**

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)

## Options

|         name          |  type  | required | default value | description |
|-----------------------|--------|----------|---------------|-------------|
| url                   | String | Yes      | -             | Enterprise WeChat robot webhook URL. |
| mentioned_list        | array  | No       | -             | User IDs to mention. Use `@all` to mention everyone. |
| mentioned_mobile_list | array  | No       | -             | Mobile phone numbers to mention. Use `@all` to mention everyone. |
| retry                 | int    | No       | -             | Maximum retry times when the HTTP request throws `IOException`. |
| retry_backoff_multiplier_ms | int | No    | 100           | Retry backoff multiplier in milliseconds. |
| retry_backoff_max_ms  | int    | No       | 10000         | Maximum retry backoff in milliseconds. |
| multi_table_sink_replica | int | No       | -             | Number of sink replicas used when writing multiple tables. |
| common-options        |        | no       | -             | Sink plugin common parameters. |

### url [string]

Enterprise WeChat webhook URL format is `https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=XXXXXX`.

### mentioned_list [array]

A list of user IDs to mention in the group. Use `@all` to mention everyone. If the user ID is unavailable, use `mentioned_mobile_list`.

### mentioned_mobile_list [array]

Mobile phone numbers to mention in the group. Use `@all` to mention everyone.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details

## Example

simple:

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

## Changelog

<ChangeLog />

