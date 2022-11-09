# Elasticsearch

> Elasticsearch source connector

## Description

Used to read data from Elasticsearch.

support version >= 2.x and < 8.x.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name        | type   | required | default value | 
|-------------|--------|----------|---------------|
| hosts       | array  | yes      | -             |
| username    | string | no       | -             |
| password    | string | no       | -             |
| index       | string | yes      | -             |
| source      | array  | no       | -             |
| scroll_time | string | no       | 1m            |
| scroll_size | int    | no       | 100           |
| schema      |        | no       | -             |



### hosts [array]
Elasticsearch cluster http address, the format is `host:port`, allowing multiple hosts to be specified. Such as `["host1:9200", "host2:9200"]`.

### username [string]
x-pack username.

### password [string]
x-pack password.

### index [string]
Elasticsearch index name, support * fuzzy matching.

### source [array]
The fields of index.
You can get the document id by specifying the field `_id`.If sink _id to other index,you need specify an alias for _id due to the Elasticsearch limit.
If you don't config source, you must config `schema`.

### scroll_time [String]
Amount of time Elasticsearch will keep the search context alive for scroll requests.

### scroll_size [int]
Maximum number of hits to be returned with each Elasticsearch scroll request.

### schema
The structure of the data, including field names and field types.
If you don't config schema, you must config `source`.

## Examples
simple
```hocon
Elasticsearch {
    hosts = ["localhost:9200"]
    index = "seatunnel-*"
    source = ["_id","name","age"]
}
```
complex
```hocon
Elasticsearch {
    hosts = ["elasticsearch:9200"]
    index = "st_index"
    schema = {
        fields {
            c_map = "map<string, tinyint>"
            c_array = "array<tinyint>"
            c_string = string
            c_boolean = boolean
            c_tinyint = tinyint
            c_smallint = smallint
            c_int = int
            c_bigint = bigint
            c_float = float
            c_double = double
            c_decimal = "decimal(2, 1)"
            c_bytes = bytes
            c_date = date
            c_timestamp = timestamp
        }
    }
}
```

## Changelog

### next version

- Add Elasticsearch Source Connector