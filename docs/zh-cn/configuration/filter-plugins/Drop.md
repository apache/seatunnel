## Filter plugin : Drop

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

丢弃掉符合指定条件的Row

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [condition](#condition-string) | string | yes | - |

##### condition [string]

条件表达式，符合此条件表达式的Row将被丢弃。条件表达式语法即sql中where条件中的条件表达式，如 `name = 'garyelephant'`, `status = '200' and resp_time > 100`

### Examples

```
drop {
    condition = "status = '200'"
}
```

> 状态码为200的Row将被丢弃
