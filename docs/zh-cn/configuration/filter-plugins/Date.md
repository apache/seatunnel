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
| [source_field](#source_field-string) | string | no | \_\_root\_\_ |
| [source_time_format](#source_time_format-string) | string | no | UNIX_MS |
| [target_field](#target_field-string) | string | no | datetime |
| [target_time_format](#target_time_format-string) | string | no | `yyyy/MM/dd HH:mm:ss` |
| [time_zone](#time_zone-string) | string | no | - |
| [common-options](#common-options-string)| string | no | - |


##### default_value [string]

如果日期转换失败将会使用当前时间生成指定格式的时间

##### locale [string]

编码类型

##### source_field [string]

源字段，若不配置将使用当前时间

##### source_time_format [string]

源字段时间格式，当前支持UNIX(10位的秒时间戳)、UNIX_MS(13位的毫秒时间戳)以及`SimpleDateFormat`时间格式。常用的时间格式列举如下：

| Symbol | Description |
| --- | --- |
| y | Year |
| M | Month |
| d | Day of month |
| H | Hour in day (0-23) |
| m | Minute in hour |
| s | Second in minute |

详细的时间格式语法见[Java SimpleDateFormat](https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html)。


##### target_field [string]

目标字段，若不配置默认为`datetime`

##### target_time_format [string]

目标字段时间格式，详细的时间格式语法见[Java SimpleDateFormat](https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html)。

##### time_zone [string]

时区

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


### Examples

```
date {
    source_field = "timestamp"
    target_field = "date"
    source_time_format = "UNIX"
    target_time_format = "yyyy/MM/dd"
}
```

> 将源数据中的`timestamp`字段由UNIX时间戳，例如*1517128894*转换为`yyyy/MM/dd`格式的`date`字段，例如*2018/01/28*

```
date {
    source_field = "httpdate"
    target_field = "datetime"
    source_time_format = "dd/MMM/yyyy:HH:mm:ss Z"
    target_time_format = "yyyy/MM/dd HH:mm:ss"
}
```

> 将源数据中的`httpdate`字段由`dd/MMM/yyyy:HH:mm:ss Z`格式转化为`yyyy/MM/dd HH:mm:ss`格式的`datetime`字段
