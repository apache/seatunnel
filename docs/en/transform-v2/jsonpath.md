# JsonPath

> JsonPath transform plugin

## Description

> Support use jsonpath select data

## Options

|  name  | type  | required | default value |
|--------|-------|----------|---------------|
| fields | Array | Yes      |               |

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

### fields[array]

**option**

|    name    |  type  | required | default value |
|------------|--------|----------|---------------|
| src_field  | String | Yes      |               |
| dest_field | String | Yes      |               |
| path       | String | Yes      |               |

**src_field**

> the json source field you want to parse

**dest_field**

> after use jsonpath output field

**path**

> Jsonpath

## Example

The data read from source is a table like this:

|                    data                     | timestamp |
|---------------------------------------------|-----------|
| {"data":{"name":"seatunnel","version":2.3}} | 10000     |
| {"data":{"name":"seatunnel","version":2.2}} | 20000     |

we want select `name` and `version` from `data`

```json
transform {
  JsonPath {
    source_table_name = "fake"
    result_table_name = "fake1"
    fields = [
     {
        "src_field" = "data"
        "path" = "$.data.name"
        "dest_field" = "name"
     },
     {
        "src_field" = "data"
        "path" = "$.data.version"
        "dest_field" = "version"
     }
    ]
  }
}
```

Then the data result table `fake1` will like this

|                    data                     | timestamp |   name    | version |
|---------------------------------------------|-----------|-----------|---------|
| {"data":{"name":"seatunnel","version":2.3}} | 10000     | seatunnel | 2.3     |
| {"data":{"name":"seatunnel","version":2.2}} | 20000     | seatunnel | 2.2     |

## Changelog

* Add JsonPath Transform

