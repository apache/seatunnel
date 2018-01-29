## Filter plugin : Remove

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

重命名数据中的字段

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_field](#source_field-array) | array | yes | - |
| [target_field](#target_field-array) | array | yes | - |

##### source_field [array]

需要重命名的字段

##### target_field [array]

变更之后的字段名

### Examples

```
rename {
    source_field = "field1"
    target_field = "field2"
}
```

> 将原始数据中的`field1`字段重命名为`field2`字段
