## Input plugin : Hdfs [Static]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.0

### Description

从HDFS文件中读取数据。注意此插件与`HdfsStream`不同，它不是流式的。


### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [format](#format-string) | string | no | json |
| [options.*](#options-object) | object | no | - |
| [options.rowTag](#optionsrowTag-string) | string | no | - |
| [path](#path-string) | string | yes | - |
| [common-options](#common-options-string)| string | yes | - |

##### format [string]

从HDFS中读取文件的格式，目前支持`csv`、`json`、`parquet` 、`xml`、`orc`和 `text`.


##### options [object]

自定义参数，当`format = "xml"`时必须设置`optionss.rowTag`，配置XML格式数据的Tag，其他参数不是必填参数。


##### options.rowTag [string]

当format为xml必须设置`optionss.rowTag`，配置XML格式数据的Tag


##### path [string]

Hadoop集群文件路径，以hdfs://开头

##### common options [string]

`Input` 插件通用参数，详情参照 [Input Plugin](/zh-cn/configuration/input-plugin)



### Example

```
hdfs {
    path = "hdfs:///var/waterdrop-logs"
    result_table_name = "access_log"
    format = "json"
}
```

> 从HDFS中读取json文件，加载到waterdrop中待后续处理.


或者可以指定 hdfs name service:

```
hdfs {
    result_table_name = "access_log"
    path = "hdfs://m2:8022/waterdrop-logs/access.log"
}
```


也支持读取XML格式的文件:

```
hdfs {
    result_table_name = "books"
    path = "hdfs://m2:8022/waterdrop-logs/books.xml"
    options.rowTag = "book"
    format = "xml"
}
```
