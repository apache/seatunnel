# Redis

> Source plugin: Redis [Spark]

## Description

Read data from Redis.

## Options

| name                | type     | required | default value |
|---------------------|----------|----------|---------------|
| host                | string   | yes      | "localhost"   |
| port                | int      | yes      | 6379          |
| auth                | string   | no       |               |
| db_num              | int      | no       | 0             |
| keys_or_key_pattern | string   | yes      |               |
| partition_num       | int      | no       | 3             |
| data_type           | string   | no       | "KV"          |
| timeout             | int      | no       | 2000          |
| common-options      | string   | yes      |               |

### host [string]

Redis server address, default `"localhost"`

### port [int]

Redis service port, default `6379`

### auth [string]

Redis authentication password

### db_num [int]

Redis database index ID. It is connected to db `0` by default

### keys_or_key_pattern [string]

Redis Key, support fuzzy matching

### partition_num [int]

Number of Redis shards. The default is `3`

### data_type [string]

Redis data type eg: `KV HASH LIST SET ZSET`

### timeout [int]

Redis timeout

### common options [string]

Source Plugin common parameters, refer to [Source Plugin](./source-plugin.md) for details

## Example

```bash
redis {
  host = "localhost"
  port = 6379
  auth = "myPassword"
  db_num = 1
  keys_or_key_pattern = "*"
  partition_num = 20
  data_type = "HASH"
  result_table_name = "hash_result_table"
}
```

> The returned table is a data table in which both fields are strings

| raw_key   | raw_message |
| --------- | ----------- |
| keys      | xxx         |
| my_keys   | xxx         |
| keys_mine | xxx         |
