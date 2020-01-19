## Source plugin : SocketStream [Spark]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 2.0.0

### Description

`SocketStream` 主要用于接受 Socket 数据，用于快速验证 Spark 流式计算。


### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [host](#host-string) | string | no | localhost |
| [port](#port-number) | number | no | 9999 |
| [common-options](#common-options-string)| string | yes | - |

##### host [string]

socket server hostname

##### port [number]

socket server port

##### common options [string]

`Source` 插件通用参数，详情参照 [Source Plugin](/zh-cn/v2/spark/configuration/source-plugins/)



### Examples

```
socketStream {
  port = 9999
}
```
