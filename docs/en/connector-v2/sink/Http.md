# Http

## Description

Used to launch web hooks using data. Both support streaming and batch mode.

> For example, if the data from upstream is [`age: 12, name: tyrantlucifer`], the body content is the following: `{"age": 12, "name": "tyrantlucifer"}`

**Tips: Http sink only support `post json` webhook and the data from source will be treated as body content in web hook.**

##  Options

| name | type   | required | default value |
| --- |--------| --- | --- |
| url | String | Yes | - |
| headers | Map    | No | - |

### url [string]

http request url

### headers [Map]

http headers

## Example

simple:

```hocon
Http {
        url = "http://localhost/test/webhook"
        headers {
            token = "9e32e859ef044462a257e1fc76730066"
        }
    }
```

