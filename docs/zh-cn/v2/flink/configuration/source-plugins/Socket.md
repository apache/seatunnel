## Source plugin : Socket [Flink]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 2.0.0

### Description
> Socket作为数据源

### Options
| name | type | required | default value |
| --- | --- | --- | --- |
| [host](#host-string) | string | no | localhost |
| [port](#port-int) | int | no | 9999 |
| [common-options](#common-options-string)| string | no | - |

##### host [string]

socket server hostname

##### port [int]

socket server port

##### common options [string]

`Source` 插件通用参数，详情参照 [Source Plugin](README.md)

### Examples
```
source {
  SocketStream{
        result_table_name = "socket"
        field_name = "info"
  }
}
```
