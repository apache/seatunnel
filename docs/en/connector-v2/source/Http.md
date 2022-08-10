# Http

> Http source connector

## Description

Used to read data from Http. Both support streaming and batch mode.

##  Options

| name | type   | required | default value |
| --- |--------| --- | --- |
| url | String | Yes | - |
| method | String | No | GET |
| headers | Map    | No | - |
| params | Map | No | - |
| body | String | No | - |

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

## Example

simple:

```hocon
Http {
        url = "http://localhost/test/query"
        method = "GET"
        headers {
            token = "9e32e859ef044462a257e1fc76730066"
        }
        params {
            id = "1"
            type = "TEST"
        }
        body = "{
            \"code\": 5945141259552,
            \"name\": \"test\"
        }"
    }
```

