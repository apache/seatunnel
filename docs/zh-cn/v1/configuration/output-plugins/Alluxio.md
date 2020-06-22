## Output plugin : Alluxio

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

输出数据到Alluxio文件

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [options](#options-object) | object | no | - |
| [partition_by](#partition_by-array) | array | no | - |
| [path](#path-string) | string | yes | - |
| [path_time_format](#path_time_format-string) | string | no | yyyyMMddHHmmss |
| [save_mode](#save_mode-string) | string | no | error |
| [serializer](#serializer-string) | string | no | json |
| [common-options](#common-options-string)| string | no | - |


##### options [object]

自定义参数

##### partition_by [array]

根据所选字段对数据进行分区

##### path [string]

Alluxio内存文件路径，以alluxio://开头

##### path_time_format [string]

当`path`参数中的格式为`xxxx-${now}`时，`path_time_format`可以指定alluxio路径的时间格式，默认值为 `yyyy.MM.dd`。常用的时间格式列举如下：

| Symbol | Description |
| --- | --- |
| y | Year |
| M | Month |
| d | Day of month |
| H | Hour in day (0-23) |
| m | Minute in hour |
| s | Second in minute |

详细的时间格式语法见[Java SimpleDateFormat](https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html)。

##### save_mode [string]

存储模式，当前支持overwrite，append，ignore以及error。每个模式具体含义见[save-modes](http://spark.apache.org/docs/2.2.0/sql-programming-guide.html#save-modes)

##### serializer [string]

序列化方法，当前支持csv、json、parquet、orc和text

##### common options [string]

`Output` 插件通用参数，详情参照 [Output Plugin](/zh-cn/v1/configuration/output-plugin)

### Note 

如果使用zookeeper控制alluxio，请将以下语句加入到start-waterdrop.sh中

```
driverJavaOpts="-Dalluxio.user.file.writetype.default=CACHE_THROUGH -Dalluxio.zookeeper.address=your.zookeeper.address:zookeeper.port -Dalluxio.zookeeper.enabled=true"
executorJavaOpts="-Dalluxio.user.file.writetype.default=CACHE_THROUGH -Dalluxio.zookeeper.address=your.zookeeper.address:zookeeper.port -Dalluxio.zookeeper.enabled=true"
```

### Example

```
alluxio {
    path = "alluxio:///var/logs-${now}"
    serializer = "json"
    path_time_format = "yyyy.MM.dd"
}
```

> 按天生成HDFS文件，例如**logs-2018.02.12**
