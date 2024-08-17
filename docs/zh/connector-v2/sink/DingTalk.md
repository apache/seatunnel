# 钉钉

> 钉钉 数据接收器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../concept/connector-v2-features.md)

## 描述

一个使用钉钉机器人发送消息的Sink插件。

## Options

|       名称       |   类型   | 是否必须 | 默认值 |
|----------------|--------|------|-----|
| url            | String | 是    | -   |
| secret         | String | 是    | -   |
| common-options |        | 否    | -   |

### url [String]

钉钉机器人地址格式为 https://oapi.dingtalk.com/robot/send?access_token=XXXXXX（String）

### secret [String]

钉钉机器人的密钥 (String)

### common options

Sink插件的通用参数，请参考 [Sink Common Options](../sink-common-options.md) 了解详情

## 任务示例

```hocon
sink {
 DingTalk {
  url="https://oapi.dingtalk.com/robot/send?access_token=ec646cccd028d978a7156ceeac5b625ebd94f586ea0743fa501c100007890"
  secret="SEC093249eef7aa57d4388aa635f678930c63db3d28b2829d5b2903fc1e5c10000"
 }
}
```

## 更新日志

### 2.2.0-beta 2022-09-26

- 添加钉钉接收器

