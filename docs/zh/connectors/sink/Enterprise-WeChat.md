import ChangeLog from '../changelog/connector-http-wechat.md';

# Enterprise WeChat

> 企业微信 接收器连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

一个将 SeaTunnel 行数据发送到企业微信机器人 webhook 的接收器插件。作业配置中的连接器标识符为
`WeChat`。每一行数据会按 `字段名: 字段值` 的格式序列化为纯文本消息，字段之间以字面的两个字符
`\n`（并非真实换行符）拼接后发送到 webhook。

该连接器继承自 [Http sink](Http.md)，并支持通用的 `multi_table_sink_replica` 选项。注意：
`retry`、`retry_backoff_multiplier_ms`、`retry_backoff_max_ms` 虽然会通过参数校验，但当前 Sink
写入器不会读取它们，因此不会进行任何重试。

> 例如，如果上游数据为 `{"alarmStatus": "firing", "alarmTime": "2022-08-03 01:38:49", "alarmContent": "The disk usage exceeds the threshold"}`，机器人收到的文本内容如下（其中的 `\n` 是字面的反斜杠 + `n` 两个字符，不是真实换行）：
>
> ```
> alarmStatus: firing\nalarmTime: 2022-08-03 01:38:49\nalarmContent: The disk usage exceeds the threshold
> ```

## 关键特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)

## 数据类型映射

连接器会把每一行渲染为一条纯文本消息，每个字段都通过 `String.valueOf(value)` 转换为字符串，
并以字面 `\n` 序列把 `字段名: 字段值` 拼接在一起。线上消息是纯文本，不存在按类型区分的 JSON 结构。

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
| retry                 | int    | 否       | -      | 该参数会通过参数校验，但当前 Sink 写入器不会读取它，因此不会进行任何重试。                                                  |
| retry_backoff_multiplier_ms | int | 否    | 100    | 该参数会通过参数校验，但当前 Sink 写入器不会应用它。                                                                      |
| retry_backoff_max_ms  | int    | 否       | 10000  | 该参数会通过参数校验，但当前 Sink 写入器不会应用它。                                                                      |
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

该参数会通过参数校验，但当前 Sink 写入器不会读取它，无论配置为何值都不会进行重试。

### retry_backoff_multiplier_ms [int]

该参数会通过参数校验，但当前 Sink 写入器不会应用它。

### retry_backoff_max_ms [int]

该参数会通过参数校验，但当前 Sink 写入器不会应用它。

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
