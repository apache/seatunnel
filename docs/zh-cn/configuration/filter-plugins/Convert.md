## Filter plugin : Convert

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

对指定字段进行类型转换

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [new_type](#new_type-string) | string | yes | - |
| [source_field](#source_field-string) | string | yes | - |
| [common-options](#common-options-string)| string | no | - |


##### new_type [string]

需要转换的结果类型，当前支持的类型有`string`、`integer`、`long`、`float`、`double`和`boolean`等

##### source_field [string]

源数据字段

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


### Examples

```
convert {
    source_field = "age"
    new_type = "integer"
}
```

> 将源数据中的`age`字段转换为`integer`类型


