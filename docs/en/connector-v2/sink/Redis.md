# Redis

> Redis sink connector

## Description

Used to write data to Redis.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [schema projection](../../concept/connector-v2-features.md)

##  Options

| name          | type   | required | default value |
|-------------- |--------|----------|---------------|
| host          | string | yes      | -             |
| port          | int    | yes      | -             |
| key           | string | yes      | -             |
| data_type     | string | yes      | -             |
| auth          | string | no       | -             |
| format        | string | no       | json          |
| common-options|        | no       | -             |

### host [string]

Redis host

### port [int]

Redis port

### key [string]

The value of key you want to write to redis. 

For example, if you want to use value of a field from upstream data as key, you can assign it to the field name.

Upstream data is the following:

| code | data           | success |
|------|----------------|---------|
| 200  | get success    | true    |
| 500  | internal error | false   |

If you assign field name to `code` and data_type to `key`, two data will be written to redis: 
1. `200 -> {code: 200, message: true, data: get success}`
2. `500 -> {code: 500, message: false, data: internal error}`

If you assign field name to `value` and data_type to `key`, only one data will be written to redis because `value` is not existed in upstream data's fields:

1. `value -> {code: 500, message: false, data: internal error}` 

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

### auth [String]

Redis authentication password, you need it when you connect to an encrypted cluster

### format [String]

The format of upstream data, now only support `json`, `text` will be supported later, default `json`.

When you assign format is `json`, for example:

Upstream data is the following:

| code | data        | success |
|------|-------------|---------|
| 200  | get success | true    |

Connector will generate data as the following and write it to redis:

```json

{"code":  200, "data":  "get success", "success":  "true"}

```

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details

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

## Changelog

### 2.2.0-beta 2022-09-26

- Add Redis Sink Connector
