## Output plugin : Elasticsearch

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

Write Rows to Elasticsearch. Support Elasticsearch >= 2.X


### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [hosts](#hosts-array) | array | yes | - |
| [index_type](#index_type-string) | string | no | log |
| [index_time_format](#index_time_format-string) | string | no | yyyy.MM.dd |
| [index](#index-string) | string | no | waterdrop |
| [es](#es-string) | string | no | - |

##### hosts [array]

Elasticsearch hosts, format as `host:port`. For example, `["host1:9200", "host2:9200"]`

##### index_type [string]

Elasticsearch index type

##### index_time_format [string]

Elasticsearch time format. If `index` likes `xxxx-${now}`, `index_time_format` can be used to specify the format of index, default is `yyyy.MM.dd`. The commonly used time formats are listed below:

| Symbol | Description |
| --- | --- |
| y | Year |
| M | Month |
| d | Day of month |
| H | Hour in day (0-23) |
| m | Minute in hour |
| s | Second in minute |

The detailed time format syntax:[Java SimpleDateFormat](https://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html).

##### index [string]

Elasticsearch index name, if you want to generate index based on time, you need to specify the field like `waterdrop-${now}`. `now` means current time.


##### es.* [string]

You can also specify multiple Elasticsearch's parameters described in [Elasticsearch Configuration](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html#cfg-mapping.

The way to specify parameters is to use the prefix "es" before the parameter. For example, `batch.size.entries` is specified as: `es.batch.size.entries = 1000`.If you do not specify these parameters, it will be set the default values according to Elasticsearch documentation


### Examples

```
elasticsearch {
    hosts = ["localhost:9200"]
    index = "waterdrop"
}
```

> Index name is `waterdrop`

```
elasticsearch {
    hosts = ["localhost:9200"]
    index = "waterdrop-${now}"
    es.batch.size.entries = 100000
    index_time_format = "yyyy.MM.dd"
}
```

> Create index by day. For example: **waterdrop-2017.11.03**
