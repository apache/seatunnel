## Filter plugin : Split

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

根据delimiter对字符串拆分

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [delimiter](#delimiter-string) | string | no | 空格 |
| [fields](#fields-array) | array | yes | - |
| [source_field](#source_field-string) | string | no | _ROOT_ |
| [target_field](#target_field-string) | string | no | raw_message |

##### delimiter [string]

分隔符，根据分隔符对输入字符串进行分隔操作，默认分隔符为单个空格

##### fields [list]

分割后的字段，若`fields`长度大于分隔结果长度，则多余字段赋值为空字符

##### source_field [string]

源字段，若不配置默认为`raw_message`

##### target_field [string]

目标字段，若不配置默认为`ROOT`

### Examples

```
Split {
    source_field = "message"
    delimiter = "&"
    fields = ["field1", "field2"]
}
```

> 将源数据中的`message`字段根据**&**进行分割，可以以`field1`或`field2`为key获取相应value

```
Split {
    source_field = "message"
    target_field = "info"
    delimiter = "&"
    fields = ["field1", "field2"]
}
```

> 将源数据中的`message`字段根据**&**进行分割，分割后的字段为`info`，可以以`info.field1`或`info.field2`为key获取相应value