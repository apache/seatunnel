# Enterprise WeChat

> Enterprise WeChat sink connector

## Description

A sink plugin which use Enterprise WeChat robot send message
> For example, if the data from upstream is [`"alarmStatus": "firing", "alarmTime": "2022-08-03 01:38:49"，"alarmContent": "The disk usage exceeds the threshold"`], the output content to WeChat Robot is the following:
> ```
> alarmStatus: firing 
> alarmTime: 2022-08-03 01:38:49
> alarmContent: The disk usage exceeds the threshold
> ```
**Tips: WeChat sink only support `string` webhook and the data from source will be treated as body content in web hook.**

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

##  Options

| name                  | type   | required | default value |
| --------------------- |--------|----------| ------------- |
| url                   | String | Yes      | -             |
| mentioned_list        | array  | No       | -             |
| mentioned_mobile_list | array  | No       | -             |
| common-options        |        | no       | -             |

### url [string]

Enterprise WeChat webhook url format is https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=XXXXXX（string）

### mentioned_list [array]

A list of userids to remind the specified members in the group (@ a member), @ all means to remind everyone. If the developer can't get the userid, he can use called_ mobile_ list

### mentioned_mobile_list [array]

Mobile phone number list, remind the group member corresponding to the mobile phone number (@ a member), @ all means remind everyone

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

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
        mentioned_list=["wangqing","@all"]
        mentioned_mobile_list=["13800001111","@all"]
    }
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Enterprise-WeChat Sink Connector

### 2.3.0-beta 2022-10-20
- [BugFix] Fix Enterprise-WeChat Sink data serialization ([2856](https://github.com/apache/incubator-seatunnel/pull/2856))
