# Http

> Http source connector

## Description

Used to read data from Http. Both support streaming and batch mode.

##  Options

| name          | type   | required | default value |
|---------------|--------|---------|---------------|
| url           | String | Yes     | -             |
| schema        | config | Yes     | -             |
| schema.fields | config | Yes     | -             |
| format        | string | No      | json          |
| method        | String | No      | get           |
| headers       | Map    | No      | -             |
| params        | Map    | No      | -             |
| body          | String | No      | -             |

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

the format of upstream data, now only support `json`

### schema [Config]

#### fields

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

