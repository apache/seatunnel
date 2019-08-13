## Output plugin : Elasticsearch

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

输出数据到Elasticsearch，支持的Elasticsearch版本为 >= 2.x 且 < 7.0.0。

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [hosts](#hosts-array) | array | yes | - |
| [index_type](#index_type-string) | string | no | log |
| [index_time_format](#index_time_format-string) | string | no | yyyy.MM.dd |
| [index](#index-string) | string | no | waterdrop |
| [es](#es-string) | string | no |  |
| [common-options](#common-options-string)| string | no | - |


##### hosts [array]

Elasticsearch集群地址，格式为host:port，允许指定多个host。如["host1:9200", "host2:9200"]。

##### index_type [string]

Elasticsearch index type

##### index_time_format [string]

当`index`参数中的格式为`xxxx-${now}`时，`index_time_format`可以指定index名称的时间格式，默认值为 `yyyy.MM.dd`。常用的时间格式列举如下：

| Symbol | Description |
| --- | --- |
| y | Year |
| M | Month |
| d | Day of month |
| H | Hour in day (0-23) |
| m | Minute in hour |
| s | Second in minute |

详细的时间格式语法见[Java SimpleDateFormat](https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html)。


##### index [string]

Elasticsearch index名称，如果需要根据时间生成index，可以指定时间变量，如：`waterdrop-${now}`。`now`代表当前数据处理的时间。

##### es.* [string]

用户还可以指定多个非必须参数，详细的参数列表见[Elasticsearch支持的参数](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html#cfg-mapping).

如指定`es.batch.size.entries`的方式是: `es.batch.size.entries = 100000`。如果不指定这些非必须参数，它们将使用官方文档给出的默认值。

##### common options [string]

`Output` 插件通用参数，详情参照 [Output Plugin](/zh-cn/configuration/output-plugin)


### Examples

```
elasticsearch {
    hosts = ["localhost:9200"]
    index = "waterdrop"
}
```

> 将结果写入Elasticsearch集群的名称为waterdrop的index中

```
elasticsearch {
    hosts = ["localhost:9200"]
    index = "waterdrop-${now}"
    es.batch.size.entries = 100000
    index_time_format = "yyyy.MM.dd"
}
```

> 按天创建索引，例如 **waterdrop-2017.11.03**
