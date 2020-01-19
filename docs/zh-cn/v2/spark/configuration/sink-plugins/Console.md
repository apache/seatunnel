## Sink plugin : Console [Spark]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 2.0.0

### Description

输出数据到标准输出/终端, 常用于debug, 能够很方便观察数据.

### Options

| name | type | required | default value | engine |
| --- | --- | --- | --- | --- |
| [limit](#limit-number) | number | no | 100 | batch/spark streaming |
| [serializer](#serializer-string) | string | no | plain | batch/spark streaming |
| [common-options](#common-options-string)| string | no | - | all streaming |

##### limit [number]

限制输出Row的条数，合法范围[-1, 2147483647], `-1`表示输出最多2147483647条Row

##### serializer [string]

输出时序列化的格式，可用的serializer包括: `json`, `plain`

##### common options [string]

`Sink` 插件通用参数，详情参照 [Sink Plugin](/zh-cn/v2/spark/configuration/sink-plugins/)


### Examples

```
stdout {
    limit = 10
    serializer = "json"
}
```

> 以Json格式输出10条数据
