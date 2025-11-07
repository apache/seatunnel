import ChangeLog from '../changelog/connector-redis.md';

# Redis

> Redis sink connector

## Description

Used to write data to Redis.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)

## Options

| name               | type    |       required        | default value |
|--------------------|---------|-----------------------|---------------|
| host               | string  | yes  when mode=single | -             |
| port               | int     | no                    | 6379          |
| key                | string  | yes                   | -             |
| data_type          | string  | yes                   | -             |
| batch_size         | int     | no                    | 10            |
| user               | string  | no                    | -             |
| auth               | string  | no                    | -             |
| db_num             | int     | no                    | 0             |
| mode               | string  | no                    | single        |
| nodes              | list    | yes when mode=cluster | -             |
| format             | string  | no                    | json          |
| expire             | long    | no                    | -1            |
| support_custom_key | boolean | no                    | false         |
| value_field        | string  | no                    | -             |
| hash_key_field     | string  | no                    | -             |
| hash_value_field   | string  | no                    | -             |
| common-options     |         | no                    | -             |

### host [string]

Redis host

### port [int]

Redis port

### key [string]

The value of key you want to write to redis.

For example, if you want to use value of a field from upstream data as key, you can assign it to the field name.

Upstream data is the following:

| code |      data      | success |
|------|----------------|---------|
| 200  | get success    | true    |
| 500  | internal error | false   |

If you assign field name to `code` and data_type to `key`, two data will be written to redis:
1. `200 -> {code: 200, data: get success, success: true}`
2. `500 -> {code: 500, data: internal error, success: false}`

If you assign field name to `value` and data_type to `key`, only one data will be written to redis because `value` is not existed in upstream data's fields:

1. `value -> {code: 500, data: internal error, success: false}`

Please see the data_type section for specific writing rules.

Of course, the format of the data written here I just take json as an example, the specific or user-configured `format` prevails.

### data_type [string]

Redis data types, support `key` `hash` `list` `set` `zset`

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
>
### batch_size [int]

ensure the batch write size in single-machine mode; no guarantees in cluster mode.

### user [string]

redis authentication user, you need it when you connect to an encrypted cluster

### auth [string]

Redis authentication password, you need it when you connect to an encrypted cluster

### db_num [int]

Redis database index ID. It is connected to db 0 by default

### mode [string]

redis mode, `single` or `cluster`, default is `single`

### nodes [list]

redis nodes information, used in cluster mode, must like as the following format:

["host1:port1", "host2:port2"]

### format [string]

The format of upstream data, now only support `json`, `text` will be supported later, default `json`.

When you assign format is `json`, for example:

Upstream data is the following:

| code |    data     | success |
|------|-------------|---------|
| 200  | get success | true    |

Connector will generate data as the following and write it to redis:

```json

{"code":  200, "data":  "get success", "success":  "true"}

```

### expire [long]

Set redis expiration time, the unit is second. The default value is -1, keys do not automatically expire by default.

### support_custom_key [boolean]

if true, the key can be customized by the field value in the upstream data.

Upstream data is the following:

| code |      data      | success |
|------|----------------|---------|
| 200  | get success    | true    |
| 500  | internal error | false   |

You can customize the Redis key using '{' and '}', and the field name in '{}' will be parsed and replaced by the field value in the upstream data. For example, If you assign field name to `{code}` and data_type to `key`, two data will be written to redis:
1. `200 -> {code: 200, data: get success, success: true}`
2. `500 -> {code: 500, data: internal error, success: false}`

Redis key can be composed of fixed and variable parts, connected by ':'. For example, If you assign field name to `code:{code}` and data_type to `key`, two data will be written to redis:
1. `code:200 -> {code: 200, data: get success, success: true}`
2. `code:500 -> {code: 500, data: internal error, success: false}`

### value_field [string]

The field of value you want to write to redis, `data_type` support `key` `list` `set` `zset`.

When you assign field name to `value` and value_field is `data` and data_type to `key`, for example:

Upstream data is the following:

| code |    data     | success |
|------|-------------|---------|
| 200  | get success | true    |

The following data will be written to redis:
1. `value -> get success`

### hash_key_field [string]

The field of hash key you want to write to redis, `data_type` support `hash`

### hash_value_field [string]

The field of hash value you want to write to redis, `data_type` support `hash`

When you assign field name to `value` and hash_key_field is `data` and hash_value_field is `success` and data_type to `hash`, for example:

Upstream data is the following:

| code |    data     | success |
|------|-------------|---------|
| 200  | get success | true    |

Connector will generate data as the following and write it to redis:

The following data will be written to redis:
1. `value -> get success | true`

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details

## Example

simple:

```hocon
Redis {
  host = localhost
  port = 6379
  key = age
  data_type = list
}
```

custom key:

```hocon
Redis {
  host = localhost
  port = 6379
  key = "name:{name}"
  support_custom_key = true
  data_type = key
}
```

custom value:

```hocon
Redis {
  host = localhost
  port = 6379
  key = person
  value_field = "name"
  data_type = key
}
```

custom HashKey and HashValue:

```hocon
Redis {
  host = localhost
  port = 6379
  key = person
  hash_key_field = "name"
  hash_value_field = "age"
  data_type = hash
}
```

## Changelog

<ChangeLog />

