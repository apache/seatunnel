## Input plugin : File [Static]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.1.1

### Description

从本地文件中读取原始数据。

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [format](#format-string) | string | no | json |
| [options.*](#options-object) | object | no | - |
| [options.rowTag](#optionsrowTag-string) | string | no | - |
| [path](#path-string) | string | yes | - |
| [common-options](#common-options-string)| string | yes | - |

##### format [string]

文件的格式，目前支持`csv`、`json`、`parquet` 、`xml`、`orc`和 `text`.


##### options.* [object]

自定义参数，当`format = "xml"`时必须设置`optionss.rowTag`，配置XML格式数据的Tag，其他参数不是必填参数。


##### options.rowTag [string]

当format为xml必须设置`optionss.rowTag`，配置XML格式数据的Tag


##### path [string]

文件路径，以file://开头


##### common options [string]

`Input` 插件通用参数，详情参照 [Input Plugin](/zh-cn/configuration/input-plugin)


### Example

```
file {
    path = "file:///var/log/access.log"
    result_table_name = "accesslog"
    format = "text"
}
```

也支持读取XML格式文件

```
file {
    path = "file:///data0/src/books.xml"
    options.rowTag = "book"
    format = "xml"
    result_table_name = "books"
}
```