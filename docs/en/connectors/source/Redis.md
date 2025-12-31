import ChangeLog from '../changelog/connector-redis.md';

# Redis

> Redis source connector

## Description

Used to read data from Redis.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name                | type   | required                       | default value |
|---------------------| ------ |--------------------------------| ------------- |
| host                | string | yes when mode=single           | -             |
| port                | int    | no                             | 6379          |
| keys                | string | yes                            | -             |
| read_key_enabled    | boolean| no                             | false         |
| key_field_name      | string | yes when read_key_enabled=true | key           |
| batch_size          | int    | yes                            | 10            |
| data_type           | string | yes                            | -             |
| user                | string | no                             | -             |
| auth                | string | no                             | -             |
| db_num              | int    | no                             | 0             |
| mode                | string | no                             | single        |
| hash_key_parse_mode | string | no                             | all           |
| nodes               | list   | yes when mode=cluster          | -             |
| schema              | config | yes when format=json           | -             |
| format              | string | no                             | json          |
| single_field_name   | string | yes when read_key_enabled=true | -             |
| field_delimiter     | string | no                             | ','           |
| common-options      |        | no                             | -             |

### host [string]

redis host

### port [int]

redis port

### hash_key_parse_mode [string]

hash key parse mode, support `all` `kv`, used to tell connector how to parse hash key.

when setting it to `all`, connector will treat the value of hash key as a row and use the schema config to parse it, when setting it to `kv`, connector will treat each kv in hash key as a row and use the schema config to parse it:

for example, if the value of hash key is the following shown:

```text
{ 
  "001": {
    "name": "tyrantlucifer",
    "age": 26
  },
  "002": {
    "name": "Zongwen",
    "age": 26
  }
}

```

if hash_key_parse_mode is `all` and schema config as the following shown, it will generate the following data:

```hocon
schema {
  fields {
    001 {
      name = string
      age = int
    }
    002 {
      name = string
      age = int
    }
  }
}

```

| 001                             | 002                       |
| ------------------------------- | ------------------------- |
| Row(name=tyrantlucifer, age=26) | Row(name=Zongwen, age=26) |

if hash_key_parse_mode is `kv` and schema config as the following shown, it will generate the following data:

```hocon
schema {
  fields {
    hash_key = string
    name = string
    age = int
  }
}

```

| hash_key | name          | age  |
| -------- | ------------- | ---- |
| 001      | tyrantlucifer | 26   |
| 002      | Zongwen       | 26   |

each kv that in hash key it will be treated as a row and send it to upstream.

**Tips: connector will use the first field information of schema config as the field name of each k that in each kv**

### keys [string]

keys pattern

### read_key_enabled [boolean]

This option determines whether the Redis source connector includes the Redis key in each output record when reading data.

When set to `true`, both the key and its associated value are included in the record.

By default (`false`), only the value is read and included.

If you are using a single-value Redis data type (such as `string`, `int`, etc.) with `read_key_enabled = true`, 
you must also specify `single_field_name` to map the value to a schema column, and `key_field_name` to map the Redis key.

Note: When `read_key_enabled = true`, the schema configuration must explicitly include the key field to correctly map the deserialized data.

Example :
```hocon
schema {
  fields {
      key = string
      value = string
  }
}
```

### key_field_name [string]

Specifies the field name to store the Redis key in the output record  when `read_key_enabled = true` or `data_type = hash`.

- When read_key_enabled = true, the default field name will be `key`.

- When data_type = hash and this option is not set, the default field name will be `hash_key`.

This field is useful when the default field name conflicts with existing schema fields, or if a more descriptive name is preferred.

Example :
```hocon
key_field_name = custom_key
hash_key_parse_mode = kv
format = "json"
schema = {
  fields {
      custom_key = string
      name = string
  }
}
```

### batch_size [int]

indicates the number of keys to attempt to return per iteration,default 10

**Tips:Redis source connector support fuzzy key matching, user needs to ensure that the matched keys are the same type**

### data_type [string]

redis data types, support `key` `hash` `list` `set` `zset`

- key

> The value of each key will be sent downstream as a single row of data.
> For example, the value of key is `SeaTunnel test message`, the data received downstream is `SeaTunnel test message` and only one message will be received.

- hash

> The hash key-value pairs will be formatted as json to be sent downstream as a single row of data.
> For example, the value of hash is `name:tyrantlucifer age:26`, the data received downstream is `{"name":"tyrantlucifer", "age":"26"}` and only one message will be received.

- list

> Each element in the list will be sent downstream as a single row of data.
> For example, the value of list is `[tyrantlucier, CalvinKirs]`, the data received downstream are `tyrantlucifer` and `CalvinKirs` and only two message will be received.

- set

> Each element in the set will be sent downstream as a single row of data
> For example, the value of set is `[tyrantlucier, CalvinKirs]`, the data received downstream are `tyrantlucifer` and `CalvinKirs` and only two message will be received.

- zset

> Each element in the sorted set will be sent downstream as a single row of data
> For example, the value of sorted set is `[tyrantlucier, CalvinKirs]`, the data received downstream are `tyrantlucifer` and `CalvinKirs` and only two message will be received.

### user [string]

redis authentication user, you need it when you connect to an encrypted cluster

### auth [string]

redis authentication password, you need it when you connect to an encrypted cluster

### db_num [int]

Redis database index ID. It is connected to db 0 by default

### mode [string]

redis mode, `single` or `cluster`, default is `single`

### nodes [list]

redis nodes information, used in cluster mode, must like as the following format:

["host1:port1", "host2:port2"]

### format [string]

the format of upstream data, now only support `json` `text`, default `json`.

when you assign format is `json`, you should also assign schema option, for example:

upstream data is the following:

```json
{"code":  200, "data":  "get success", "success":  true}

```

you should assign schema as the following:

```hocon
schema {
    fields {
        code = int
        data = string
        success = boolean
    }
}

```

connector will generate data as the following:

| code | data        | success |
| ---- | ----------- | ------- |
| 200  | get success | true    |

when you assign format is `text`, you can choose to specify the schema information or not. 

For example, upstream data is the following:

```text
200#get success#true
```

If you do not assign data schema connector will treat the upstream data as the following:

| content                                                  |
| -------------------------------------------------------- |
| 200#get success#true |

If you assign data schema, you should also assign the option `schema` and `field_delimiter` as following:

```hocon
field_delimiter = "#"
schema {
    fields {
        code = int
        data = string
        success = boolean
    }
}

```
connector will generate data as the following:

| content                                                  |
| -------------------------------------------------------- |
| {"code":  200, "data":  "get success", "success":  true} |

### field_delimiter [string]
Field delimiter, used to tell connector how to slice and dice fields.

Currently, only need to be configured when format is text. default is ",".

### schema [config]

#### fields [config]

the schema fields of redis data

### single_field_name [string]

Specifies the field name for Redis values when `read_key_enabled = true` and the value is a single primitive (e.g., `string`, `int`).

This name is used in the schema to map the value field.

**Note:** This option has no effect when reading complex Redis data types such as hashes or objects that can be directly mapped to a schema.

Example :
```hocon
read_key_enabled = true
key_field_name = key
single_field_name = value
schema {
  fields {
    key = string
    value = string
  }
}
```

### common options

Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details

## Example

simple:

```hocon
Redis {
  host = localhost
  port = 6379
  keys = "key_test*"
  data_type = key
  format = text
}
```

```hocon
Redis {
  host = localhost
  port = 6379
  keys = "key_test*"
  data_type = key
  format = json
  schema {
    fields {
      name = string
      age = int
    }
  }
}
```

read string type keys write append to list

```hocon
source {
  Redis {
    host = "redis-e2e"
    port = 6379
    auth = "U2VhVHVubmVs"
    keys = "string_test*"
    data_type = string
    batch_size = 33
  }
}

sink {
  Redis {
    host = "redis-e2e"
    port = 6379
    auth = "U2VhVHVubmVs"
    key = "string_test_list"
    data_type = list
    batch_size = 33
  }
}
```

## Changelog

<ChangeLog />