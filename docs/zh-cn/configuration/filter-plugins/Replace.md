## Filter plugin : Replace

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

将指定字段内容根据正则表达式进行替换

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [pattern](#pattern-string) | string | yes | - |
| [replacement](#replacement-string) | string | yes | - |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | replaced |
| [common-options](#common-options-string)| string | no | - |


##### pattern [string]

用于做匹配的正则表达式。常见的书写方式如 `"[a-zA-Z0-9_-]+"`, 详见[Regex Pattern](https://docs.oracle.com/javase/8/docs/api/java/util/regex/Pattern.html)。
也可以到这里测试正则表达式是正确：[Regex 101](https://regex101.com/)

##### replacement [string]

替换的字符串

##### source_field [string]

源字段，若不配置默认为`raw_message`

##### target_field [string]

目标字段，若不配置默认为`replaced`

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


### Examples

```
replace {
    target_field = "tmp"
    source_field = "message"
    pattern = "is"
    replacement = "are"
}
```
> 将`message`中的**is**替换为**are**，并赋值给`tmp`
