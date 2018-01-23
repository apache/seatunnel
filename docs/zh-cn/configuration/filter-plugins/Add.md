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

##### target_field [string]

新增的字段名

##### value [string]

新增字段的值, 目前仅支持固定值，不支持变量。

### Examples

```
add {
    flag = 1
}
```

> 新增一个字段，其值为1
