## Input plugin : File [Static]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

从本地文件中读取原始数据。

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [format](#format-string) | string | yes | json |
| [path](#path-string) | string | yes | - |
| [table_name](#table_name-string) | string | yes | - |

##### format [string]

文件格式，默认是json，支持text、csv等

##### path [string]

文件路径

##### table_name [string]

注册的表名，可为任意字符串

### Example

```
file {
    path = "file:///var/log/access.log"
    table_name = "accesslog"
    format = "text"
}
```
