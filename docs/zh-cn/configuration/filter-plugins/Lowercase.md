## Filter plugin : Lowercase

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

将指定字段内容全部转换为小写字母

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_field](#source_field-string) | string | no | raw_message |
| [target_field](#target_field-string) | string | no | lowercased |
| [common-options](#common-options-string)| string | no | - |


##### source_field [string]

源字段，若不配置默认为`raw_message`

##### target_field [string]

目标字段，若不配置默认为`lowercased`

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


# Examples

```
lowercase {
    source_field = "address"
    target_field = "address_lowercased"
}
```
