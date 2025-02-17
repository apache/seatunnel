# JsonPath

> JsonPath transform plugin

## Description

> Support use jsonpath select data

## Options

| name                 | type  | required | default value |
|----------------------|-------|----------|---------------|
| columns              | Array | Yes      |               |
| row_error_handle_way | Enum  | No       | FAIL          |

### common options [string]

Transform plugin common parameters, please refer to [Transform Plugin](common-options.md) for details

### row_error_handle_way [Enum]

This option is used to specify the processing method when an error occurs in the row, the default value is `FAIL`.

- FAIL: When `FAIL` is selected, data format error will block and an exception will be thrown.
- SKIP: When `SKIP` is selected, data format error will skip this row data.

### columns[array]

#### option

| name                    | type   | required | default value |
|-------------------------|--------|----------|---------------|
| src_field               | String | Yes      |               |
| dest_field              | String | Yes      |               |
| path                    | String | Yes      |               |
| dest_type               | String | No       | String        |
| column_error_handle_way | Enum   | No       |               |

#### src_field

> the json source field you want to parse

Support SeatunnelDateType

* STRING
* BYTES
* ARRAY
* MAP
* ROW

#### dest_field

> after use jsonpath output field

#### dest_type

> the type of dest field

#### path

> Jsonpath

#### column_error_handle_way [Enum]

This option is used to specify the processing method when an error occurs in the column.

- FAIL: When `FAIL` is selected, data format error will block and an exception will be thrown.
- SKIP: When `SKIP` is selected, data format error will skip this column data.
- SKIP_ROW: When `SKIP_ROW` is selected, data format error will skip this row data.

## Read Json Example

The data read from source is a table like this json:

```json
{
  "data": {
    "c_string": "this is a string",
    "c_boolean": true,
    "c_integer": 42,
    "c_float": 3.14,
    "c_double": 3.14,
    "c_decimal": 10.55,
    "c_date": "2023-10-29",
    "c_datetime": "16:12:43.459",
    "c_array":["item1", "item2", "item3"],
    "c_map_array": [{"c_string_1":"c_string_1","c_string_2":"c_string_2","c_string_3":"c_string_3"},{"c_string_1":"c_string_1","c_string_2":"c_string_2","c_string_3":"c_string_3"}]
  }
}
```

Assuming we want to use JsonPath to extract properties.

```json
transform {
  JsonPath {
    plugin_input = "fake"
    plugin_output = "fake1"
    columns = [
     {
        "src_field" = "data"
        "path" = "$.data.c_string"
        "dest_field" = "c1_string"
     },
     {
        "src_field" = "data"
        "path" = "$.data.c_boolean"
        "dest_field" = "c1_boolean"
        "dest_type" = "boolean"
     },
     {
        "src_field" = "data"
        "path" = "$.data.c_integer"
        "dest_field" = "c1_integer"
        "dest_type" = "int"
     },
     {
        "src_field" = "data"
        "path" = "$.data.c_float"
        "dest_field" = "c1_float"
        "dest_type" = "float"
     },
     {
        "src_field" = "data"
        "path" = "$.data.c_double"
        "dest_field" = "c1_double"
        "dest_type" = "double"
     },
      {
         "src_field" = "data"
         "path" = "$.data.c_decimal"
         "dest_field" = "c1_decimal"
         "dest_type" = "decimal(4,2)"
      },
      {
         "src_field" = "data"
         "path" = "$.data.c_date"
         "dest_field" = "c1_date"
         "dest_type" = "date"
      },
      {
         "src_field" = "data"
         "path" = "$.data.c_datetime"
         "dest_field" = "c1_datetime"
         "dest_type" = "time"
      },
      {
         "src_field" = "data"
         "path" = "$.data.c_array"
         "dest_field" = "c1_array"
         "dest_type" = "array<string>"        
      },
      {
        "src_field" = "data"
        "path" = "$.data.c_map_array"
        "dest_field" = "c1_map_array"
        "dest_type" = "array<map<string, string>>"
      }
    ]
  }
}
```

Then the data result table `fake1` will like this

|             data             |    c1_string     | c1_boolean | c1_integer | c1_float | c1_double | c1_decimal |  c1_date   | c1_datetime  |          c1_array           |
|------------------------------|------------------|------------|------------|----------|-----------|------------|------------|--------------|-----------------------------|
| too much content not to show | this is a string | true       | 42         | 3.14     | 3.14      | 10.55      | 2023-10-29 | 16:12:43.459 | ["item1", "item2", "item3"] |

## Read SeatunnelRow Example

Suppose a column in a row of data is of type SeatunnelRow and that the name of the column is col

<table>
<tr><th colspan="2">SeatunnelRow(col)</th><th>other</th></tr>
<tr><td>name</td><td>age</td><td>....</td></tr>
<tr><td>a</td><td>18</td><td>....</td></tr>
</table>

The JsonPath transform converts the values of seatunnel into an array,

```hocon
transform {
  JsonPath {
    plugin_input = "fake"
    plugin_output = "fake1"
  
    row_error_handle_way = FAIL
    columns = [
     {
        "src_field" = "col"
        "path" = "$[0]"
        "dest_field" = "name"
        "dest_type" = "string"
     },
     {
        "src_field" = "col"
        "path" = "$[1]"
        "dest_field" = "age"
        "dest_type" = "int"
     }
    ]
  }
}
```

Then the data result table `fake1` will like this

| name | age |   col    | other |
|------|-----|----------|-------|
| a    | 18  | ["a",18] | ...   |


## Configure error data handle way

You can configure `row_error_handle_way` and `column_error_handle_way` to handle abnormal data. Both are optional.

`row_error_handle_way` is used to handle all data anomalies in the row data, while `column_error_handle_way` is used to handle data anomalies in a column. It has a higher priority than `row_error_handle_way`.

### Skip error data rows

Configure to skip row data with exceptions in any column

```hocon
transform {
  JsonPath {

    row_error_handle_way = SKIP
    
    columns = [
     {
        "src_field" = "json_data"
        "path" = "$.f1"
        "dest_field" = "json_data_f1"
     },
     {
        "src_field" = "json_data"
        "path" = "$.f2"
        "dest_field" = "json_data_f2"
     }
    ]
  }
}
```

### Skip error data column

Configure only `json_data_f1` column data exceptions to skip and fill in null values, other column data exceptions will continue to throw exception interrupt handlers


```hocon
transform {
  JsonPath {

    row_error_handle_way = FAIL
    
    columns = [
     {
        "src_field" = "json_data"
        "path" = "$.f1"
        "dest_field" = "json_data_f1"
        
        "column_error_handle_way" = "SKIP"
     },
     {
        "src_field" = "json_data"
        "path" = "$.f2"
        "dest_field" = "json_data_f2"
     }
    ]
  }
}
```

### Skip the row for specified column error

Configure to skip the row of data only for `json_data_f1` column data exceptions, and continue to throw exceptions to interrupt the handler for other column data exceptions


```hocon
transform {
  JsonPath {

    row_error_handle_way = FAIL
    
    columns = [
     {
        "src_field" = "json_data"
        "path" = "$.f1"
        "dest_field" = "json_data_f1"
        
        "column_error_handle_way" = "SKIP_ROW"
     },
     {
        "src_field" = "json_data"
        "path" = "$.f2"
        "dest_field" = "json_data_f2"
     }
    ]
  }
}
```

## Changelog

* Add JsonPath Transform

