# Intro to schema feature

## Why we need schema

Some NoSQL databases or message queue are not strongly limited schema, so the schema cannot be obtained through the api. At this time, a schema needs to be defined to convert to SeaTunnelRowType and obtain data.

## What type supported at now

| Data type | Description                                                  |
| :-------- | :----------------------------------------------------------- |
| string    | string                                                       |
| boolean   | boolean                                                      |
| tinyint   | -128 to 127 regular. 0 to 255 unsigned*. Specify the maximum number of digits in parentheses. |
| smallint  | -32768 to 32767 General. 0 to 65535 unsigned*. Specify the maximum number of digits in parentheses. |
| int       | All numbers from -2,147,483,648 to 2,147,483,647 are allowed. |
| bigint    | All numbers between -9,223,372,036,854,775,808 and 9,223,372,036,854,775,807 are allowed. |
| float     | Float-precision numeric data from -1.79E+308 to 1.79E+308.   |
| double    | Double precision floating point. Handle most decimals.       |
| decimal   | DOUBLE type stored as a string, allowing a fixed decimal point. |
| null      | null                                                         |
| bytes     | bytes.                                                       |
| date      | Only the date is stored. From January 1, 0001 to December 31, 9999. |
| time      | Only store time. Accuracy is 100 nanoseconds.                |
| timestamp | Stores a unique number that is updated whenever a row is created or modified. timestamp is based on the internal clock and does not correspond to real time. There can only be one timestamp variable per table. |
| row       | Row type,can be nested.                                      |
| map       | A Map is an object that maps keys to values. The key type includes `int` `string` `boolean` `tinyint` `smallint` `bigint` `float` `double` `decimal` `date` `time` `timestamp` `null` , and the value type includes `int` `string` `boolean` `tinyint` `smallint` `bigint` `float` `double` `decimal` `date` `time` `timestamp` `null` `array` `map`. |
| array     | A array is a data type that represents a collection of elements. The element type includes `int` `string` `boolean` `tinyint` `smallint` `bigint` `float` `double` `array` `map`. |

## How to use schema

`schema` defines the format of the data,it contains`fields` properties. `fields` define the field properties,it's a K-V key-value pair, the Key is the field name and the value is field type. Here is an example.

```
source {
  FakeSource {
    parallelism = 2
    result_table_name = "fake"
    row.num = 16
    schema = {
      fields {
        id = bigint
        c_map = "map<string, smallint>"
        c_array = "array<tinyint>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(2, 1)"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
}
```

## When we should use it or not

If there is a `schema` configuration project in Options,the connector can then customize the schema. Like `Fake` `Pulsar` `Http` source connector etc. 
