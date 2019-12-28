## Filter plugin : Remove

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

重命名数据中的字段

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [source_field](#source_field-string) | array | yes | - |
| [target_field](#target_field-string) | array | yes | - |
| [common-options](#common-options-string)| string | no | - |


##### source_field [string]

需要重命名的字段

##### target_field [string]

变更之后的字段名

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


### Examples

```
rename {
    source_field = "field1"
    target_field = "field2"
}
```

> 将原始数据中的`field1`字段重命名为`field2`字段
