# Redis

> Redis sink connector

## Description

Write Rows to a Redis.

:::tip

Engine Supported and plugin name

* [x] Spark: Redis
* [ ] Flink

:::

## Options

| name      | type   | required | default value |
|-----------|--------|----------|---------------|
| host      | string | no       | "localhost"   |
| port      | int    | no       | 6379          |
| auth      | string | no       |               |
| db_num    | int    | no       | 0             |
| data_type | string | no       | "KV"          |
| hash_name | string | no       |               |
| list_name | string | no       |               |
| set_name  | string | no       |               |
| zset_name | string | no       |               |
| timeout   | int    | no       | 2000          |
| ttl       | int    | no       | 0             |
| is_self_achieved    | boolean | no       | false         |

### host [string]

Redis server address, default `"localhost"`

### port [int]

Redis service port, default `6379`

### auth [string]

Redis authentication password

### db_num [int]

Redis database index ID. It is connected to db `0` by default

### redis_timeout [int]

Redis timeout

### data_type [string]

Redis data type eg: `KV HASH LIST SET ZSET`

### hash_name [string]

if redis data type is HASH must config hash name 

### list_name [string]

if redis data type is LIST must config list name

### zset_name [string]

if redis data type is ZSET must config zset name

### set_name [string]

if redis data type is SET must config set name

### ttl [int]

redis data expiration ttl, 0 means no expiration.

### is_self_achieved [boolean]

If a redis access by a self-achieved redis proxy, which is not support redis function of "info Replication"

## Examples

```bash
redis {
  host = "localhost"
  port = 6379
  auth = "myPassword"
  db_num = 1
  data_type = "HASH"
  hash_name = "test"
  is_self_achieved = false
}
```
