## Filter plugin : Watermark

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.3.0

### Description

Spark Structured Streaming Watermark

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [event_time](#event_time-string) | string | yes |  |
| [delay_threshold](#delay_threshold-string) | string | yes |  |

##### event_time [string]

日志中的事件时间，必须为timestamp格式。

##### delay_threshold [string]

等待数据到达的最小延迟。

### Example

```
Watermark {
    event_time = "datetime"
    delay_threshold = "5 minutes"
}
```
