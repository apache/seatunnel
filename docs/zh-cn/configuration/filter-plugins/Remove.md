## Filter plugin : Remove

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

删除数据中的字段

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_field](#source_field-array) | array | yes | - |

##### source_field [array]

需要删除的字段列表

### Examples

```
remove {
    source_field = ["field1", "field2"]
}
```

> 删除原始数据中的`field1`和`field2`字段
