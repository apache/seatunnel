# Elasticsearch

## Description

Read data from `Elasticsearch`.

:::tip

Engine Supported

* supported  `ElasticSearch version is >= 2.x and < 8.x`

:::

## Options

| name        | type   | required | default value | 
|-------------|--------| -------- |---------------|
| hosts       | array  | yes      | -             |
| username    | string | no       |               |
| password    | string | no       |               |
| index       | string | yes      | -             |
| source      | array  | yes      | -             |
| scroll_time | string | no       | 1m            |
| scroll_size | int    | no       | 100           |



### hosts [array]
`Elasticsearch` cluster http address, the format is `host:port` , allowing multiple hosts to be specified. Such as `["host1:9200", "host2:9200"]`.

### username [string]
x-pack username

### password [string]
x-pack password

### index [string]
`Elasticsearch` index name, support * fuzzy matching

### source [array]
The fields of index.
You can get the document id by specifying the field `_id`.If sink _id to other index,you need specify an alias for _id due to the `Elasticsearch` limit.

### scroll_time [String]
Amount of time `Elasticsearch` will keep the search context alive for scroll requests.

### scroll_size [int]
Maximum number of hits to be returned with each `Elasticsearch` scroll request.

## Examples
```bash
Elasticsearch {
    hosts = ["localhost:9200"]
    index = "seatunnel-*"
    source = ["_id","name","age"]
}
```
