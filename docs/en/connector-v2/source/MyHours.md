# My Hours

> My Hours source connector

## Description

Used to read data from My Hours.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

##  Options

| name                        | type   | required | default value |
| --------------------------- | ------ | -------- | ------------- |
| email                       | String | Yes      | -             |
| password                    | String | Yes      | -             |
| projects                    | String | No       | -             |
| users                       | String | No       | -             |
| schema.fields               | Config | No       | -             |
| format                      | String | No       | json          |
| retry                       | int    | No       | -             |
| retry_backoff_multiplier_ms | int    | No       | 100           |
| retry_backoff_max_ms        | int    | No       | 10000         |
| common-options              |        | No       | -             |

### email [String]

email for login

### password [String]

password for login

### projects [String]

Get the information on your Projects

Projects can be configured as `all` or `active`

If `all` is configured, all projects will be queried

If configured as `active`, only the currently incomplete projects will be displayed

### users [String]

Get the users information on your account

Users can be configured as `member`  or `client`

If `all` is configured, list all team members in the account

If configured as `active`, list all clients in the account

### retry [int]

The max retry times if request http return to `IOException`

### retry_backoff_multiplier_ms [int]

The retry-backoff times(millis) multiplier if request http failed

### retry_backoff_max_ms [int]

The maximum retry-backoff times(millis) if request http failed

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
MyHours{
    email = "seatunnel@test.com"
    password = "seatunnel"
    projects = "active"
    schema {
       fields {
           clientId = string
           clientName = string
           name = string
           archived = boolean
           id = int
       }
    }
}
```

## Changelog

### 2.2.0-beta 2022-10-22

- Add My Hours Source Connector
