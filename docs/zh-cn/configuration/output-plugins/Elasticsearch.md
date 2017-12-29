## Output plugin : Elasticsearch

* Author: rickyHuo
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

输出Dataframe到Elasticsearch，Elasticsearch支持的[参数](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html#cfg-mapping)

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [hosts](#hosts-array) | array | yes | - |
| [index_type](#index_type-string) | string | no | log |
| [index_time_format](#index_time_format-string) | string | no | yyyy.MM.dd |
| [index](#index-string) | string | no | waterdrop |

##### hosts [array]

Elasticsearch集群地址，格式为host:port

##### index_type [string]

Elasticsearch索引类型

##### index_time_format [string]

时间格式，当需要根据时间格式生成`index`时配置使用

##### index [string]

Elasticsearch索引，如果需要根据时间生成索引，配置样例`log-${now}`

### Examples

```
Elasticsearch {
    hosts = ["localhost:9200"]
    index = "waterdrop"
}
```

> 将结果写入Elasticsearch集群waterdrop索引中

```
Elasticsearch {
    hosts = ["localhost:9200"]
    index = "waterdrop-${now}"
    index_time_format = "yyyy.mm"
}
```

> 按月建立索引，例如**waterdrop-2017.11**
