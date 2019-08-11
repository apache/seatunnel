## Filter plugin : Split

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

根据delimiter分割字符串。

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [delimiter](#delimiter-string) | string | no | " "(空格) |
| [fields](#fields-array) | array | yes | - |
| [source_field](#source_field-string) | string | no | _root_ |
| [target_field](#target_field-string) | string | no | raw_message |
| [common-options](#common-options-string)| string | no | - |


##### delimiter [string]

分隔符，根据分隔符对输入字符串进行分隔操作，默认分隔符为一个空格(" ")。

##### fields [list]

分割后的字段名称列表，按照顺序指定被分割后的各个字符串的字段名称。
若`fields`长度大于分隔结果长度，则多余字段赋值为空字符。

##### source_field [string]

被分割前的字符串来源字段，若不配置默认为`raw_message`

##### target_field [string]

`target_field` 可以指定被分割后的多个字段被添加到Event的位置，若不配置默认为`_root_`，即将所有分割后的字段，添加到Event最顶级。
如果指定了特定的字段，则被分割后的字段将被添加到这个字段的下面一级。

##### common options [string]

`Filter` 插件通用参数，详情参照 [Filter Plugin](/zh-cn/configuration/filter-plugin)


### Examples

```
split {
    source_field = "message"
    delimiter = "&"
    fields = ["field1", "field2"]
}
```

> 将源数据中的`message`字段根据**&**进行分割，可以以`field1`或`field2`为key获取相应value

```
split {
    source_field = "message"
    target_field = "info"
    delimiter = ","
    fields = ["field1", "field2"]
}
```

> 将源数据中的`message`字段根据**,**进行分割，分割后的字段为`info`，可以以`info.field1`或`info.field2`为key获取相应value
