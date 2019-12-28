## Filter plugin : Add

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

在源数据中新增一个字段

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [target_field](#target_field-string) | string | yes | - |
| [value](#value-string) | string | yes | - |
| [common-options](#common-options-string)| string | no | - |

##### target_field [string]

新增的字段名

##### value [string]

新增字段的值, 目前仅支持固定值，不支持变量

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)

### Examples

```
add {
    value = "1"
}
```

> 新增一个字段，其值为1
