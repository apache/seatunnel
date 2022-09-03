# Http

> Http source connector

## Description

Used to read data from Http.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [schema projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)

##  Options

| name          | type   | required | default value |
|---------------|--------|----------|---------------|
| url           | String | Yes      | -             |
| schema        | config | No       | -             |
| schema.fields | config | No       | -             |
| format        | string | No       | json          |
| method        | String | No       | get           |
| headers       | Map    | No       | -             |
| params        | Map    | No       | -             |
| body          | String | No       | -             |

### url [string]

http request url

### method [string]

http request method, only supports GET, POST method.

### headers [Map]

http headers

### params [Map]

http params

### body [String]

http body

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

## Example

simple:

```hocon
Http {
    url = "https://tyrantlucifer.com/api/getDemoData"
    schema {
      fields {
        code = int
        message = string
        data = string
        ok = boolean
      }
    }
}
```

