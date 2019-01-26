## Input plugin : FileStream [Streaming]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.0

### Description

从本地文件目录中读取原始数据，会监听新文件生成。

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [format](#format-string) | no | yes | text |
| [path](#path-string) | string | yes | - |
| [rowTag](#rowtag-string) | no | yes | - |


##### format [string]

文件格式


##### path [string]

文件目录路径


##### rowTag [string]

仅当format为xml时使用，表示XML格式数据的Tag

### Example

```
fileStream {
    path = "file:///var/log/"
}
```

或者指定`format`

```
fileStream {
    path = "file:///var/log/"
    format = "xml"
    rowTag = "book"
}
```
