## Input plugin : Elasticsearch [Static]

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.3.2

### Description

从 Elasticsearch 中读取数据

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [hosts](#hosts-array) | array | yes | - |
| [index](#index-string) | string | yes |  |
| [es](#es-string) | string | no |  |
| [table_name](#table_name-string) | string | yes | - |

##### hosts [array]

ElasticSearch 集群地址，格式为host:port，允许指定多个host。如 \["host1:9200", "host2:9200"]。


##### index [string]

ElasticSearch index名称，支持 `*` 模糊匹配


##### es.* [string]

用户还可以指定多个非必须参数，详细的参数列表见[Elasticsearch支持的参数](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html#cfg-mapping).

如指定 `es.read.metadata` 的方式是: `es.read.metadata = true`。如果不指定这些非必须参数，它们将使用官方文档给出的默认值。

##### table_name [string]

数据在 Spark 中注册的表名

### Examples

```
elasticsearch {
    hosts = ["localhost:9200"]
    index = "waterdrop-20190424"
    table_name = "my_dataset"
  }
```


```
elasticsearch {
    hosts = ["localhost:9200"]
    index = "waterdrop-*"
    es.read.field.include = "name, age"
    table_name = "my_dataset"
  }
```

> 匹配所有以 `waterdrop-` 开头的索引， 并且仅仅读取 `name`和 `age` 两个字段。
