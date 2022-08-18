# Redis

> Redis source connector

## Description

Read data from Redis.

:::tip

Engine Supported and plugin name

* [x] Spark: Redis
* [ ] Flink

:::

## Options

| name                | type    | required | default value |
|---------------------|---------|----------|---------------|
| host                | string  | no       | "localhost"   |
| port                | int     | no       | 6379          |
| auth                | string  | no       |               |
| db_num              | int     | no       | 0             |
| keys_or_key_pattern | string  | yes      |               |
| partition_num       | int     | no       | 3             |
| data_type           | string  | no       | "KV"          |
| timeout             | int     | no       | 2000          |
| common-options      | string  | yes      |               |
| is_self_achieved    | boolean | no       | false         |

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

Source Plugin common parameters, refer to [Source Plugin](common-options.mdx) for details

### is_self_achieved [boolean]

If a redis access by a self-achieved redis proxy, which is not support redis function of "info Replication"

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
  is_self_achieved = false
}
```

> The returned table is a data table in which both fields are strings

| raw_key   | raw_message |
| --------- | ----------- |
| keys      | xxx         |
| my_keys   | xxx         |
| keys_mine | xxx         |
