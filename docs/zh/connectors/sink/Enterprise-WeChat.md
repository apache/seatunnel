import ChangeLog from '../changelog/connector-http-wechat.md';

# Enterprise WeChat

> 企业微信 接收器连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

一个将 SeaTunnel 行数据发送到企业微信机器人 webhook 的接收器插件。作业配置中的连接器标识符为
`WeChat`。每一行数据会按 `字段名: 字段值` 的格式序列化为多行纯文本消息，再发送到 webhook。

该连接器继承自 [Http sink](Http.md)，因此自带标准的 HTTP 重试参数（`retry`、
`retry_backoff_multiplier_ms`、`retry_backoff_max_ms`），并支持通用的
`multi_table_sink_replica` 选项以调整多表写入的并行度。

> 例如，如果上游数据为 `{"alarmStatus": "firing", "alarmTime": "2022-08-03 01:38:49", "alarmContent": "The disk usage exceeds the threshold"}`，企业微信机器人收到的内容如下：
>
> ```
> alarmStatus: firing
> alarmTime: 2022-08-03 01:38:49
> alarmContent: The disk usage exceeds the threshold
> ```

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

## 数据类型映射

连接器会把每一行渲染为一条纯文本消息，每个字段都通过 `String.valueOf(value)` 转换为字符串，
并以 `字段名: 字段值` 的格式独立成行。线上消息是纯文本，不存在按类型区分的 JSON 结构。

| SeaTunnel 数据类型 | 企业微信消息字段 |
|--------------------|-----------------|
| string             | `字段名: string` |
| tinyint / smallint / int / bigint | `字段名: number` |
| float / double     | `字段名: number` |
| boolean            | `字段名: true/false` |
| date / time / timestamp | `字段名: ISO 字符串` |
| bytes / array / map / row | `字段名: String(toString)` |

## 选项

| 名称                  | 类型   | 是否必填 | 默认值 | 描述                                                                                                                |
|-----------------------|--------|----------|--------|---------------------------------------------------------------------------------------------------------------------|
| url                   | String | 是       | -      | 企业微信机器人 webhook URL，格式 `https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=XXXXXX`。                    |
| mentioned_list        | array  | 否       | -      | 需要提醒的用户 ID 列表，使用 `@all` 提醒所有人。                                                                     |
| mentioned_mobile_list | array  | 否       | -      | 需要提醒的手机号列表，使用 `@all` 提醒所有人。                                                                      |
| retry                 | int    | 否       | -      | HTTP 请求抛出 `IOException` 时的最大重试次数。默认不重试。                                                          |
| retry_backoff_multiplier_ms | int | 否    | 100    | 重试退避倍数，单位毫秒。                                                                                            |
| retry_backoff_max_ms  | int    | 否       | 10000  | 最大重试退避时间，单位毫秒。                                                                                        |
| multi_table_sink_replica | int | 否       | 1      | 多表写入时使用的写入器副本数。                                                                                      |
| common-options        |        | 否       | -      | 接收器插件通用参数，详见 [Sink 通用选项](../common-options/sink-common-options.md)。                                |

### url [string]

企业微信 webhook URL，格式 `https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=XXXXXX`。`key`
查询参数是在企业微信群机器人设置中生成的机器人 key。

### mentioned_list [array]

需要提醒的用户 ID 列表，使用 `@all` 提醒所有人。如果无法获取用户 ID，可以使用
`mentioned_mobile_list`。

### mentioned_mobile_list [array]

需要提醒的手机号列表，使用 `@all` 提醒所有人。

### retry [int]

HTTP 请求抛出 `IOException` 时的最大重试次数，默认不重试。重试间隔由
`retry_backoff_multiplier_ms` 和 `retry_backoff_max_ms` 共同决定。

### retry_backoff_multiplier_ms [int]

重试退避的基础单位，单位毫秒。重试之间的等待时间会在多次重试中逐渐增长，上限为
`retry_backoff_max_ms`。增长曲线并不是每次固定的倍数关系，具体的斐波那契策略请参考
`HttpClientProvider`（位于 `connector-http-base`）。默认 `100`。

### retry_backoff_max_ms [int]

最大重试退避时间，单位毫秒。默认 `10000`。

### multi_table_sink_replica [int]

多表写入时使用的写入器副本数。增加该值可以在每个表上启动更多并行写入器。默认 `1`。

### common options

接收器插件通用参数，详见 [Sink 通用选项](../common-options/sink-common-options.md)。

## 任务示例

### 简单示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 1
    schema = {
      fields {
        alarmStatus = string
        alarmTime = string
        alarmContent = string
      }
    }
    rows = [
      {
        fields = ["firing", "2022-08-03 01:38:49", "The disk usage exceeds the threshold"]
      }
    ]
  }
}

sink {
  WeChat {
    url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=693axxx6-7aoc-4bc4-97a0-0ec2sifa5aaa"
  }
}
```

### 同时 @ 指定用户和手机号

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 1
    schema = {
      fields {
        alarmStatus = string
        alarmTime = string
        alarmContent = string
      }
    }
    rows = [
      {
        fields = ["firing", "2022-08-03 01:38:49", "The disk usage exceeds the threshold"]
      }
    ]
  }
}

sink {
  WeChat {
    url = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=693axxx6-7aoc-4bc4-97a0-0ec2sifa5aaa"
    mentioned_list = ["wangqing", "@all"]
    mentioned_mobile_list = ["13800001111", "@all"]
  }
}
```

## 变更日志

<ChangeLog />
