# DataHub

> DataHub 接收器连接器

## 描述

一个使用向 DataHub 发送消息的接收器插件

## 关键特性

- [ ] [精确一次](../../concept/connector-v2-features.md)

## 选项

|      名称           |  类型  | 必需 | 默认值 |
|----------------|--------|----|---------------|
| endpoint       | string | 是  | -             |
| accessId       | string | 是  | -             |
| accessKey      | string | 是  | -             |
| project        | string | 是  | -             |
| topic          | string | 是  | -             |
| timeout        | int    | 是  | -             |
| retryTimes     | int    | 是  | -             |
| common-options |        | 否  | -             |

### endpoint [string]

您的DataHub端点以http开头

### accessId [string]

您的DataHub accessId可以从阿里云访问哪个云

### accessKey[string]

您的DataHub accessKey可以从阿里云访问哪个云

### project [string]

您在阿里云中创建的DataHub项目

### topic [string]

您的DataHub主题

### timeout [int]

最大连接超时

### retryTimes [int]

客户端放置记录失败时的最大重试次数

### common options

接收器插件常用参数，详见 [Sink Common Options](../sink-common-options.md) 

## 示例

```hocon
sink {
 DataHub {
  endpoint="yourendpoint"
  accessId="xxx"
  accessKey="xxx"
  project="projectname"
  topic="topicname"
  timeout=3000
  retryTimes=3
 }
}
```

## 变更日志

### 2.2.0-beta 2022-09-26

- 添加DataHub接收器连接器

