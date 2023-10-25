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

#### option

|    name    |  type  | required | default value |
|------------|--------|----------|---------------|
| src_field  | String | Yes      |               |
| dest_field | String | Yes      |               |
| path       | String | Yes      |               |

#### src_field

> the json source field you want to parse

Support SeatunnelDateType

* STRING
* BYTES
* ARRAY
* MAP
* ROW

#### dest_field

> after use jsonpath output field,dest_field type is always STRING

#### path

> Jsonpath

## Example1

The data read from source is a table like this:

|                    dATA                     | timestamp |
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

## Example2

The data read from source is a table like this:

|   data(Seatunnelrow)   | timestamp |
|------------------------|-----------|
| ["data1",2,"data3"]    | 1000      |
| ["data21",22,"data23"] | 2000      |

the first column in the given rows is also of type `seatunnelRow`, and we want use JsonPath to select data from the first column.

```json
transform {
  JsonPath {
    source_table_name = "fake"
    result_table_name = "fake1"
    fields = [
     {
        "src_field" = "data"
  //We select the data at index zero.
        "path" = "$[0]"
        "dest_field" = "test_str"
     }
    ]
  }
}
```

Then the data result table `fake1` will like this

|          data          | timestamp | test_str |
|------------------------|-----------|----------|
| ["data1",2,"data3"]    | 1000      | data1    |
| ["data21",22,"data23"] | 2000      | Data2    |

## Changelog

* Add JsonPath Transform

