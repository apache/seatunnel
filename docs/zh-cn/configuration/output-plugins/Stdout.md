## Output plugin : Stdout

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

输出数据到标准输出

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [limit](#limit-number) | number | no | 100 |
| [serializer](#serializer-string) | string | no | plain |

##### limit [number]

限制输出Row的条数，合法范围[-1, 2147483647], `-1`表示输出最多2147483647条Row

##### serializer [string]

输出时序列化的格式，可选的序列化方式请见：[Serializers](/#/zh-cn/)

### Example

```
stdout {
    limit = 10
    serializer = "json"
}
```

> 以Json格式输出10条数据