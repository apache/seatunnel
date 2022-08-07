# Elasticsearch

> Elasticsearch source connector

## Description

Read data from Elasticsearch

:::tip 

Engine Supported and plugin name

* [x] Spark: Elasticsearch
* [ ] Flink

:::

## Options

| name           | type   | required | default value |
| -------------- | ------ | -------- | ------------- |
| hosts          | array  | yes      | -             |
| index          | string | yes      |               |
| es.*           | string | no       |               |
| common-options | string | yes      | -             |

### hosts [array]

ElasticSearch cluster address, the format is host:port, allowing multiple hosts to be specified. Such as `["host1:9200", "host2:9200"]` .

### index [string]

ElasticSearch index name, support * fuzzy matching

### es.* [string]

Users can also specify multiple optional parameters. For a detailed list of parameters, see [Parameters Supported by Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html#cfg-mapping).

For example, the way to specify `es.read.metadata` is: `es.read.metadata = true` . If these non-essential parameters are not specified, they will use the default values given in the official documentation.

### common options [string]

Source plugin common parameters, please refer to [Source Plugin](common-options.mdx) for details

## Examples

```bash
elasticsearch {
    hosts = ["localhost:9200"]
    index = "seatunnel-20190424"
    result_table_name = "my_dataset"
}
```

```bash
elasticsearch {
    hosts = ["localhost:9200"]
    index = "seatunnel-*"
    es.read.field.include = "name, age"
    resulttable_name = "my_dataset"
}
```

> Matches all indexes starting with `seatunnel-` , and only reads the two fields `name` and `age` .
