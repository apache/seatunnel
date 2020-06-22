## Input plugin : Alluxio [Static]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.0

### Description

从Alluxio文件中读取数据。


### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [format](#format-string) | string | no | json |
| [options.*](#options-object) | object | no | - |
| [options.rowTag](#optionsrowTag-string) | string | no | - |
| [path](#path-string) | string | yes | - |
| [common-options](#common-options-string)| string | yes | - |

##### format [string]

从Alluxio中读取文件的格式，目前支持`csv`、`json`、`parquet` 、`xml`、`orc`和 `text`.


##### options [object]

自定义参数，当`format = "xml"`时必须设置`optionss.rowTag`，配置XML格式数据的Tag，其他参数不是必填参数。


##### options.rowTag [string]

当format为xml必须设置`optionss.rowTag`，配置XML格式数据的Tag


##### path [string]

Alluxio内存文件路径，以alluxio://开头

##### common options [string]

`Input` 插件通用参数，详情参照 [Input Plugin](/zh-cn/v1/configuration/input-plugin)

### Note 

如果使用zookeeper控制alluxio，请将以下语句加入到start-waterdrop.sh中

```
driverJavaOpts="-Dalluxio.user.file.writetype.default=CACHE_THROUGH -Dalluxio.zookeeper.address=your.zookeeper.address:zookeeper.port -Dalluxio.zookeeper.enabled=true"
executorJavaOpts="-Dalluxio.user.file.writetype.default=CACHE_THROUGH -Dalluxio.zookeeper.address=your.zookeeper.address:zookeeper.port -Dalluxio.zookeeper.enabled=true"
```

### Example

```
alluxio {
    path = "alluxio:///var/waterdrop-logs"
    result_table_name = "access_log"
    format = "json"
}
```

> 从Alluxio中读取json文件，加载到waterdrop中待后续处理.


或者可以指定 alluxio name service:

```
alluxio {
    result_table_name = "access_log"
    path = "alluxio://m2:19999/waterdrop-logs/access.log"
}
```


也支持读取XML格式的文件:

```
alluxio {
    result_table_name = "books"
    path = "alluxio://m2:19999/waterdrop-logs/books.xml"
    options.rowTag = "book"
    format = "xml"
}
```
