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
| [common-options](#common-options-string)| string | no | - |


##### source_field [array]

需要删除的字段列表

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


### Examples

```
remove {
    source_field = ["field1", "field2"]
}
```

> 删除原始数据中的`field1`和`field2`字段
