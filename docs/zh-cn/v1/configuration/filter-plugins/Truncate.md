## Filter plugin : Truncate

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

对指定字段进行字符串截取

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [max_length](#max_length-number) | number | no | 256 |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | truncated |
| [common-options](#common-options-string)| string | no | - |


##### max_length [number]

截取字符串的最大长度

##### source_field [string]

源字段，若不配置默认为`raw_message`

##### target_field [string]

目标字段，若不配置默认为`__root__`

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


### Example

```
truncate {
    source_field = "telephone"
    max_length = 10
}
```
