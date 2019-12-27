## Filter plugin : Watermark

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.3.0

### Description

Spark Structured Streaming Watermark

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [time_field](#time_field-string) | string | yes | - |
| [time_type](#time_type-string) | string | no | UNIX |
| [time_pattern](#time_pattern-string) | string | no | yyyy-MM-dd HH:mm:ss |
| [delay_threshold](#delay_threshold-string) | string | yes | - |
| [watermark_field](#watermark_field-string) | string | yes | - |
| [common-options](#common-options-string)| string | no | - |


##### time_field [string]

日志中的事件时间字段

##### time_type [string]

日志中的事件时间字段的类型，支持三种类型 `UNIX_MS|UNIX|string`，UNIX_MS为13位的时间戳，UNIX为10位的时间戳，string为字符串类型的时间,如2019-04-08 22:10:23

##### time_pattern [string]

当你的`time_type`选择为string时，你可以指定这个参数来进行时间字符串的匹配，默认匹配格式为yyyy-MM-dd HH:mm:ss

##### delay_threshold [string]

等待数据到达的最小延迟。

##### watermark_field [string]

经过这个filter处理之后将会增加一个timestamp类型的字段，这个字段用于添加watermark

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


### Example

```
Watermark {
         delay_threshold = "5 minutes"
         time_field = "tf"
         time_type = "UNIX"
         watermark_field = "wm"
}
```
