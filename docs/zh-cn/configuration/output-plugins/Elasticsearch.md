## Output plugin : Stdout

* Author: rickyHuo
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

输出Dataframe到Elasticsearch

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [hosts](#hosts-array) | array | yes | - |
| [index](#index-string) | string | yes | - |
| [index_type](#index_type-string) | string | yes | - |

##### hosts [array]

Elasticsearch集群地址，格式为host:port

##### index [string]

Elasticsearch index

##### index_type [string]

Elasticsearch index type
