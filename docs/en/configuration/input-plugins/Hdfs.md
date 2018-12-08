## Input plugin : Hdfs

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.0

### Description

Read raw data from HDFS.

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [path](#path-string) | string | yes | - |

##### path [string]

File path on Hadoop cluster.

### Example

```
hdfs {
    path = "hdfs:///access.log"
}
```

or you can specify hdfs name service:

```
hdfs {
    path = "hdfs://m2:8022/access.log"
}
```
