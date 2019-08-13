## Input plugin : HdfsStream [Streaming]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.0

### Description

监听HDFS目录中的文件变化，实时加载并处理新文件，形成文件处理流。

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [format](#format-string) | no | yes | text |
| [path](#path-string) | string | yes | - |
| [rowTag](#rowtag-string) | no | yes | - |
| [common-options](#common-options-string)| string | yes | - |


##### format [string]

文件格式


##### path [string]

文件目录路径


##### rowTag [string]

仅当format为xml时使用，表示XML格式数据的Tag

##### common options [string]

`Input` 插件通用参数，详情参照 [Input Plugin](/zh-cn/configuration/input-plugin)


### Example

```
hdfsStream {
    path = "hdfs:///access/log/"
}
```

或者可以指定 hdfs name service:

```
hdfsStream {
    path = "hdfs://m2:8022/access/log/"
}
```

或者指定`format`

```
hdfsStream {
    path = "hdfs://m2:8022/access/log/"
    format = "xml"
    rowTag = "book"
}
```