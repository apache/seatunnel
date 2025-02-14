# Enterprise WeChat

> Enterprise WeChat 接收器连接器

## 描述

一个使用 Enterprise WeChat 机器人发送消息的接收插件

> 例如，如果来自上游的数据是 [`"alarmStatus": "firing", "alarmTime": "2022-08-03 01:38:49"，"alarmContent": "The disk usage exceeds the threshold"`], 微信机器人的输出内容如下:
>
> ```
> alarmStatus: firing 
> alarmTime: 2022-08-03 01:38:49
> alarmContent: The disk usage exceeds the threshold
> ```
>
> **小贴士: WeChat 接收器仅支持 `string` 类型 webhook ，源数据将被视为webhook中的正文内容.**

## 关键特性

- [ ] [精确一次](../../concept/connector-v2-features.md)

## 选项

|         名称           |  类型  | 必需 | 默认值 |
|-----------------------|--------|----|---------------|
| url                   | String | 是  | -             |
| mentioned_list        | array  | 否  | -             |
| mentioned_mobile_list | array  | 否 | -             |
| common-options        |        | 否 | -             |

### url [string]

企业微信网络挂钩 url 格式为 https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=XXXXXX（string）

### mentioned_list [array]

一个用户标识列表，用于提醒组中的指定成员（@A成员），@all意味着提醒每个人。如果开发人员无法获得用户ID，他可以使用called_mobile_list

### mentioned_mobile_list [array]

手机号码列表，提醒群组成员对应的手机号码（@a成员），@all表示提醒大家

### common options

接收器插件常用参数，详见 [Sink Common Options](../sink-common-options.md) 

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
        mentioned_list=["wangqing","@all"]
        mentioned_mobile_list=["13800001111","@all"]
    }
```

## 变更日志

### 2.2.0-beta 2022-09-26

- 添加 Enterprise-WeChat 接收器连接器

### 2.3.0-beta 2022-10-20

- [Bug修复] 修复企业微信Sink数据序列化问题 ([2856](https://github.com/apache/seatunnel/pull/2856))

