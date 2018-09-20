## Input plugin : Hdfs [Static]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

从HDFS文件中读取数据。注意此插件与`HdfsStream`不同，它不是流式的。


### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [options](#options-object) | object | no | - |
| [path](#path-string) | string | yes | - |
| [format](#format-string) | string | no | json |
| [table_name](#table_name-string) | string | yes | - |

##### options [object]

自定义参数

##### path [string]

Hadoop集群文件路径，以hdfs://开头

##### format [string]

从HDFS中读取文件的格式，目前支持`csv`、`json`、`parquet` 和 `text`.

##### table_name [string]

注册的表名


### Example

```
hdfs {
    path = "hdfs:///var/waterdrop-logs"
    table_name = "access_log"
    format = "json"
}
```

> 从HDFS中读取json文件，加载到waterdrop中待后续处理.


或者可以指定 hdfs name service:

```
hdfs {
    table_name = "access_log"
    path = "hdfs://m2:8022/waterdrop-logs/access.log"
}
```
