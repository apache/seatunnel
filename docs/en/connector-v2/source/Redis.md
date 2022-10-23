# Redis

> Redis source connector

## Description

Used to read data from Redis.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

##  Options

| name           | type   | required | default value |
|--------------- |--------|----------|---------------|
| host           | string | yes      | -             |
| port           | int    | yes      | -             |
| keys           | string | yes      | -             |
| data_type      | string | yes      | -             |
| auth           | string | No       | -             |
| schema         | config | No       | -             |
| format         | string | No       | json          |
| common-options |        | no       | -             |

### host [string]

redis host

### port [int]

redis port

### keys [string]

keys pattern

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

### auth [String]

redis authentication password, you need it when you connect to an encrypted cluster

### format [String]

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
|------|-------------|---------|
| 200  | get success | true    |

when you assign format is `text`, connector will do nothing for upstream data, for example:

upstream data is the following:

```json

{"code":  200, "data":  "get success", "success":  true}

```

connector will generate data as the following:

| content |
|---------|
| {"code":  200, "data":  "get success", "success":  true}        |

### schema [Config]

#### fields [Config]

the schema fields of upstream data

### common options 

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details

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

## Changelog

### 2.2.0-beta 2022-09-26

- Add Redis Source Connector
