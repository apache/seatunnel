## Input plugin : HdfsStream [Streaming]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.0

### Description

监听HDFS目录中的文件变化，实时加载并处理新文件，形成文件处理流。

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [path](#path-string) | string | yes | - |

##### path [string]

Hadoop集群上文件路径

### Example

```
hdfsStream {
    path = "hdfs:///access.log"
}
```

或者可以指定 hdfs name service:

```
hdfsStream {
    path = "hdfs://m2:8022/access.log"
}
```
