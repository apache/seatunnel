## Input plugin : FileStream [Streaming]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

从本地文件中读取原始数据

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [path](#path-string) | string | yes | - |

##### path [string]

文件路径

### Example

```
fileStream {
    path = "file:///var/log/access.log"
}
```
