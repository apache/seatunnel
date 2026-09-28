import ChangeLog from '../changelog/connector-http-wechat.md';

# Enterprise WeChat

> Enterprise WeChat sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

A sink plugin that sends SeaTunnel rows to an Enterprise WeChat robot webhook. The connector
identifier in job configuration is `WeChat`. Each row is serialized into a plain-text message with
the fields rendered as `fieldName: fieldValue` lines before the request is sent to the webhook.

The connector extends the [Http sink](Http.md) so it inherits the standard HTTP retry behaviour
(`retry`, `retry_backoff_multiplier_ms`, `retry_backoff_max_ms`) and the generic
`multi_table_sink_replica` option for fan-out writes.

> For example, if the data from upstream is `{"alarmStatus": "firing", "alarmTime": "2022-08-03 01:38:49", "alarmContent": "The disk usage exceeds the threshold"}`, the output content to WeChat Robot is the following:
>
> ```
> alarmStatus: firing
> alarmTime: 2022-08-03 01:38:49
> alarmContent: The disk usage exceeds the threshold
> ```

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)

## Data Type Mapping

The connector renders every upstream row as one plain-text message. Each field is converted to its
string representation (`String.valueOf(value)`) and emitted on its own line together with the
field name, so no per-type JSON structure exists on the wire.

| SeaTunnel Data Type | Enterprise WeChat Message Field |
|---------------------|---------------------------------|
| string              | `fieldName: string`             |
| tinyint / smallint / int / bigint | `fieldName: number` |
| float / double      | `fieldName: number`             |
| boolean             | `fieldName: true/false`         |
| date / time / timestamp | `fieldName: ISO string`     |
| bytes / array / map / row | `fieldName: String(toString)` |

## Options

|         name          |  type  | required | default value | description                                                                                                              |
|-----------------------|--------|----------|---------------|--------------------------------------------------------------------------------------------------------------------------|
| url                   | String | Yes      | -             | Enterprise WeChat robot webhook URL, format `https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=XXXXXX`.               |
| mentioned_list        | array  | No       | -             | User IDs to mention in the group. Use `@all` to mention everyone.                                                        |
| mentioned_mobile_list | array  | No       | -             | Mobile phone numbers to mention in the group. Use `@all` to mention everyone.                                            |
| retry                 | int    | No       | -             | Maximum retry times when the HTTP request throws `IOException`.                                                          |
| retry_backoff_multiplier_ms | int | No    | 100           | Retry backoff multiplier in milliseconds.                                                                                |
| retry_backoff_max_ms  | int    | No       | 10000         | Maximum retry backoff in milliseconds.                                                                                   |
| multi_table_sink_replica | int | No       | 1             | Number of writer replicas used when writing multiple tables.                                                              |
| common-options        |        | no       | -             | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details. |

### url [string]

Enterprise WeChat webhook URL format is
`https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=XXXXXX`. The `key` query parameter is the
robot key generated in the Enterprise WeChat group robot settings.

### mentioned_list [array]

A list of user IDs to mention in the group. Use `@all` to mention everyone. If the user ID is
unavailable, use `mentioned_mobile_list`.

### mentioned_mobile_list [array]

Mobile phone numbers to mention in the group. Use `@all` to mention everyone.

### retry [int]

Maximum retry times when the HTTP request throws `IOException`. There is no retry by default. The
retry loop uses `retry_backoff_multiplier_ms` and `retry_backoff_max_ms` to compute the wait
between attempts.

### retry_backoff_multiplier_ms [int]

Base unit (in milliseconds) for the retry backoff. The wait between attempts grows across retries
up to `retry_backoff_max_ms`. The growth curve is not a fixed multiplier per attempt — see
`HttpClientProvider` (`connector-http-base`) for the exact Fibonacci-based strategy. Default is `100`.

### retry_backoff_max_ms [int]

Maximum wait between retries, in milliseconds. Default is `10000`.

### multi_table_sink_replica [int]

Number of writer replicas used when writing multiple tables. Increase this value to add more
parallel writers per table. Default is `1`.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Task Example

### Simple

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

### Mention users and phone numbers

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

## Changelog

<ChangeLog />
