# Elasticsearch

## Description

Output data to `Elasticsearch`.

## Key features

- [ ] [exactly-once](key-features.md)

- [ ] [schema projection](key-features.md)

:::tip

Engine Supported

* supported  `ElasticSearch version is >= 2.x and < 8.x`

:::

## Options

| name              | type   | required | default value | 
|-------------------| ------ | -------- |---------------|
| hosts             | array  | yes      | -             |
| index             | string | yes      | -             |
| index_type        | string | no       |               |
| username          | string | no       |               |
| password          | string | no       |               | 
| max_retry_size    | int    | no       | 3             |
| max_batch_size    | int    | no       | 10            |



### hosts [array]
`Elasticsearch` cluster http address, the format is `host:port` , allowing multiple hosts to be specified. Such as `["host1:9200", "host2:9200"]`.

### index [string]
`Elasticsearch`  `index` name.Index support contains variables of field name,such as `seatunnel_${age}`,and the field must appear at seatunnel row.

### index_type [string]
`Elasticsearch` index type, it is recommended not to specify in elasticsearch 6 and above

### username [string]
x-pack username

### password [string]
x-pack password

### max_retry_size [int]
one bulk request max try size

### max_batch_size [int]
batch bulk doc max size

## Examples
```bash
Elasticsearch {
    hosts = ["localhost:9200"]
    index = "seatunnel-${age}"
}
```
