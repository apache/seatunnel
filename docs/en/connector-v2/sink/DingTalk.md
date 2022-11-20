# DingTalk

> DinkTalk sink connector

## Description

A sink plugin which use DingTalk robot send message

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

## Options

| name             | type        | required | default value |
|------------------| ----------  | -------- | ------------- |
| url              | string      | yes      | -             |
| secret           | string      | yes      | -             |
| common-options   |             | no       | -             |

### url [string]

DingTalk robot address format is https://oapi.dingtalk.com/robot/send?access_token=XXXXXX（string）

### secret [string]

DingTalk robot secret (string)

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

## Example

```hocon
sink {
 DingTalk {
  url="https://oapi.dingtalk.com/robot/send?access_token=ec646cccd028d978a7156ceeac5b625ebd94f586ea0743fa501c100007890"
  secret="SEC093249eef7aa57d4388aa635f678930c63db3d28b2829d5b2903fc1e5c10000"
 }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add DingTalk Sink Connector
