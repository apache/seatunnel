# Redis

> Redis sink connector

## Description

Used to write data to Redis.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

##  Options

| name      | type   | required | default value |
|-----------|--------|----------|---------------|
| host      | string | yes      | -             |
| port      | int    | yes      | -             |
| keys      | string | yes      | -             |
| data_type | string | yes      | -             |
| auth      | string | No       | -             |
| format    | string | No       | json          |

### host [string]

redis host

### port [int]

redis port

### keys [string]

keys pattern

**Tips:Redis sink connector not support fuzzy key matching, user needs to configure the specified key**

### data_type [string]

redis data types, support `key` `hash` `list` `set` `zset`

- key
> Each data from upstream will be updated to the configured key, which means the later data will overwrite the earlier data, and only the last data will be stored in the key.

- hash
> Each data from upstream will be split according to the field and written to the hash key, also the data after will overwrite the data before.

- list
> Each data from upstream will be added to the configured list key.

- set
> Each data from upstream will be added to the configured set key.

- zset
> Each data from upstream will be added to the configured zset key with a weight of 1. So the order of data in zset is based on the order of data consumption.

### auth [String]

redis authentication password, you need it when you connect to an encrypted cluster

### format [String]

the format of upstream data, now only support `json`, `text` will be supported later, default `json`.

when you assign format is `json`, for example:

upstream data is the following:

| code | data        | success |
|------|-------------|---------|
| 200  | get success | true    |

connector will generate data as the following and write it to redis:

```json

{"code":  200, "data":  "get success", "success":  "true"}

```

## Example

simple:

```hocon
  Redis {
    host = localhost
    port = 6379
    keys = "key_test"
    data_type = key
  }
```

