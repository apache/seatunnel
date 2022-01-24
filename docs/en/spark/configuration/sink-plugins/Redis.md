# Sink plugin: Redis [Spark]

## Description

Write Rows to a Redis.

## Options

| name                                       | type   | required | default value |
|--------------------------------------------|--------|----------|---------------| 
| [redis_host](#redis_host-string)           | string | yes      | "localhost"   |
| [redis_port](#redis_port-string)           | string | yes      | "6379"        | 
| [redis_auth](#redis_auth-string)           | string | no       | -             | 
| [redis_db](#redis_db-int)                  | int    | no       | 0             | 
| [redis_timeout](#redis_timeout-int)        | int    | no       | 2000          | 
| [redis_save_type](#redis_save_type-string) | string | yes      | -             | 
| [redis_hash_name](#redis_hash_name-string) | string | no       | -             |
| [redis_list_name](#redis_list_name-string) | string | no       | -             | 
| [redis_zset_name](#redis_zset_name-string) | string | no       | -             | 
| [redis_set_name](#redis_set_name-string)   | string | no       | -             |

### redis_host [string]

redis host

### redis_port [string]

redis port

### redis_auth [string]

redis password

### redis_db [int]

redis database

### redis_timeout [int]

redis timeout

### redis_save_type [string]

redis save type eg: KV HASH LIST SET ZSET

### redis_hash_name [string]

if redis save type is HASH must config hash name 

### redis_list_name [string]

if redis save type is list must config list name

### redis_zset_name [string]

if redis save type is zset must config zset name

### redis_set_name [string]

if redis save type is set must config set name

## Examples

```bash
Redis {
  redis_host = "localhost"
  redis_port = 6379
  redis_auth = "myPassword"
  redis_save_type = "HASH"
  redis_hash_name = "test"
}
```
