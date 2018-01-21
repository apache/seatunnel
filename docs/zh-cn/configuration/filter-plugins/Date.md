## Filter plugin : Date

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

对指定字段进行时间格式转换

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [default_value](#default_value-string) | string | no | ${now} |
| [locale](#locale-string) | string | no | Locale.US |
| [source_field](#source_field-string) | string | no | __ROOT__ |
| [source_time_format](#source_time_format-string) | string | no | UNIX_MS |
| [target_field](#target_field-string) | string | no | datetime |
| [target_time_format](#target_time_format-string) | string | no | `yyyy/MM/dd HH:mm:ss` |
| [time_zone](#time_zone-string) | string | no |  |

##### default_value [string]

如果日期转换失败将会使用当前时间生成指定格式的时间

##### locale [string]

编码类型

##### source_field [string]

源字段，若不配置将使用当前时间

##### source_time_format [string]

源字段时间格式，当前支持UNIX、UNIX_MS以及`SimpleDateFormat`格式

##### target_field [string]

目标字段，若不配置默认为`datetime`

##### target_time_format [string]

目标字段时间格式

##### time_zone [string]

时区


### Examples

```
date {
    source_field = "timestamp"
    target_field = "date"
    source_field_format = "UNIX"
    target_field_format = "yyyy/MM/dd"
}
```

> 将源数据中的`timestamp`字段由UNIX时间戳转换为`yyyy/MM/dd`格式的`date`字段

