## Input plugin : S3Stream [Streaming]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.0

### Description

从S3云存储上读取原始数据

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [path](#path-string) | string | yes | - |
| [common-options](#common-options-string)| string | yes | - |


##### path [string]

S3云存储路径，当前支持的路径格式有**s3://**, **s3a://**, **s3n://**

##### common options [string]

`Input` 插件通用参数，详情参照 [Input Plugin](/zh-cn/configuration/input-plugin)


### Example

```
s3Stream {
    path = "s3n://bucket/access.log"
}
```