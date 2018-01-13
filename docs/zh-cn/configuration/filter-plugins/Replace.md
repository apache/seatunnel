## Filter plugin : Lowercase

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

##### pattern [string]

正则表达式

##### replacement [string]

替换的字符串

##### source_field [string]

源字段，若不配置默认为`raw_message`

##### target_field [string]

目标字段，若不配置默认为`replaced`

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
