## Filter plugin : Uuid

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

为原始数据集新增一个全局唯一且自增的UUID字段，使用的是spark的`monotonically_increasing_id()`函数。

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [target_field](#target_field-string) | string | no | uuid |
| [common-options](#common-options-string)| string | no | - |


##### target_field [string]

存储uuid的目标字段，若不配置默认为`uuid`

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


### Example

```
uuid {
    target_field = "id"
}
```