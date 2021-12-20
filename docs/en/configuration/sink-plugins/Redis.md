# Sink plugin: Redis

### Description

Write Rows to a Redis.

### Options

| name | type | required | default value | engine |
| --- | --- | --- | --- | --- |
| [redis_host](#redis-host-string) | string | yes | "localhost" | all streaming |
| [redis_port](#redis-port-string) | string | yes | "6379" | all streaming |
| [redis_save_type](#redis-save-type-string) | string | yes | - | all streaming |
| [redis_hash_name](#redis-hash-key-name-string) | string | no | - | all streaming |
| [redis_list_name](#redis-list-key-name-string) | string | no | - | all streaming |
| [redis_zset_name](#redis-zset-key-name-string) | string | no | - | all streaming |
| [redis_set_name](#redis-set-key-name-string) | string | no | - | all streaming |

##### redis_host [string]

redis host

##### redis_port [string]

redis port

##### redis_save_type [string]

redis save type   eg: KV HASH LIST SET ZSET


##### redis_hash_name [string]

if redis save type is HASH must config hash name 

##### redis_list_name [string]

if redis save type is list must config list name

##### redis_zset_name [string]

if redis save type is zset must config zset name

##### redis_set_name [string]

if redis save type is set must config set name

### Examples

```
   Redis {
     redis_host = "localhost"
     redis_port = 6379
     redis_save_type = "HASH"
     redis_hash_name = "test"
   }
```