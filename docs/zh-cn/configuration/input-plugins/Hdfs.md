## Input plugin : Hdfs

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

从HDFS中读取原始数据

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [path](#path-string) | string | yes | - |

##### path [string]

Hadoop集群上文件路径

### Example

```
hdfs {
    path = "hdfs:///access.log"
}
```

或者可以指定 hdfs name service:

```
hdfs {
    path = "hdfs://m2:8022/access.log"
}
```
