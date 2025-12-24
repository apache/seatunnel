# Intro To Schema Feature

## Why We Need Schema

Some NoSQL databases or message queue are not strongly limited schema, so the schema cannot be obtained through the api.
At this time, a schema needs to be defined to convert to TableSchema and obtain data.

## SchemaOptions

We can use SchemaOptions to define schema, the SchemaOptions contains some configs to define the schema. e.g. columns, primaryKey, constraintKeys.

```
schema = {
    table = "database.schema.table"
    schema_first = false
    comment = "comment"
    columns = [
    ...
    ]
    primaryKey {
    ...
    }
    
    constraintKeys {
    ...
    }
}
```

### table

The table full name of the table identifier which the schema belongs to, it contains database, schema, table name. e.g. `database.schema.table`, `database.table`, `table`.

### schema_first

Default is false.

If the schema_first is true, the schema will be used first, this means if we set `table = "a.b"`, `a` will be parsed as schema rather than database, then we can support write `table = "schema.table"`.

### comment

The comment of the CatalogTable which the schema belongs to.

### Columns

Columns is a list of configs used to define the column in schema, each column can contains name, type, nullable, defaultValue, comment field.

```
columns = [
       {
          name = id
          type = bigint
          nullable = false
          columnLength = 20
          defaultValue = 0
          comment = "primary key id"
       }
]
```

| Field        | Required | Default Value |                                   Description                                    |
|:-------------|:---------|:--------------|----------------------------------------------------------------------------------|
| name         | Yes      | -             | The name of the column                                                           |
| type         | Yes      | -             | The data type of the column                                                      |
| nullable     | No       | true          | If the column can be nullable                                                    |
| columnLength | No       | 0             | The length of the column which will be useful when you need to define the length |
| columnScale  | No       | -             | The scale of the column which will be useful when you need to define the scale   |
| defaultValue | No       | null          | The default value of the column                                                  |
| comment      | No       | null          | The comment of the column                                                        |

#### What type supported at now

| Data type    | Value type in Java                                 | Description                                                                                                                                                                                                                                                                                                                                                 |
|:-------------|:---------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| string       | `java.lang.String`                                 | string                                                                                                                                                                                                                                                                                                                                                      |
| boolean      | `java.lang.Boolean`                                | boolean                                                                                                                                                                                                                                                                                                                                                     |
| tinyint      | `java.lang.Byte`                                   | -128 to 127 regular. 0 to 255 unsigned*. Specify the maximum number of digits in parentheses.                                                                                                                                                                                                                                                               |
| smallint     | `java.lang.Short`                                  | -32768 to 32767 General. 0 to 65535 unsigned*. Specify the maximum number of digits in parentheses.                                                                                                                                                                                                                                                         |
| int          | `java.lang.Integer`                                | All numbers from -2,147,483,648 to 2,147,483,647 are allowed.                                                                                                                                                                                                                                                                                               |
| bigint       | `java.lang.Long`                                   | All numbers between -9,223,372,036,854,775,808 and 9,223,372,036,854,775,807 are allowed.                                                                                                                                                                                                                                                                   |
| float        | `java.lang.Float`                                  | Float-precision numeric data from -1.79E+308 to 1.79E+308.                                                                                                                                                                                                                                                                                                  |
| double       | `java.lang.Double`                                 | Double precision floating point. Handle most decimals.                                                                                                                                                                                                                                                                                                      |
| decimal      | `java.math.BigDecimal`                             | Double type stored as a string, allowing a fixed decimal point.                                                                                                                                                                                                                                                                                             |
| null         | `java.lang.Void`                                   | null                                                                                                                                                                                                                                                                                                                                                        |
| bytes        | `byte[]`                                           | bytes                                                                                                                                                                                                                                                                                                                                                       |
| date         | `java.time.LocalDate`                              | Only the date is stored. From January 1, 0001 to December 31, 9999.                                                                                                                                                                                                                                                                                         |
| time         | `java.time.LocalTime`                              | Only store time. Accuracy is 100 nanoseconds.                                                                                                                                                                                                                                                                                                               |
| timestamp    | `java.time.LocalDateTime`                          | Stores date and time information without time zone. Represents the time of an event in local time. It does not include any offset or zone information.                                                                                                                                           |
| timestamp_tz | `java.time.OffsetDateTime`                         | Stores date and time information with an offset from UTC. It includes both the local date-time and the offset from UTC, providing more precise temporal information when working with multiple time zones.                                                                                     |
| row          | `org.apache.seatunnel.api.table.type.SeaTunnelRow` | Row type, can be nested.                                                                                                                                                                                                                                                                                                                                    |
| map          | `java.util.Map`                                    | A Map is an object that maps keys to values. The key type includes `int` `string` `boolean` `tinyint` `smallint` `bigint` `float` `double` `decimal` `date` `time` `timestamp` `null` , and the value type includes `int` `string` `boolean` `tinyint` `smallint` `bigint` `float` `double` `decimal` `date` `time` `timestamp` `null` `array` `map` `row`. |
| array        | `ValueType[]`                                      | A array is a data type that represents a collection of elements. The element type includes `int` `string` `boolean` `tinyint` `smallint` `bigint` `float` `double`.                                                                                                                                                                                         |

#### How to declare type supported

SeaTunnel provides a simple and direct way to declare basic types. Basic type keywords include `string`, `boolean`, `tinyint`, `smallint`, `int`, `bigint`, `float`, `double`, `date`, `time`, `timestamp`, and `null`. The keyword names for basic types can be used directly as type declarations, and SeaTunnel is case-insensitive to type keywords. For example, if you need to declare a field with integer type, you can simply define the field as `int` or `"int"`.

> The null type declaration must be enclosed in double quotes, like `"null"`. This approach helps avoid confusion with [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md)'s `null` type which represents undefined object.

When declaring complex types (such as **decimal**, **array**, **map**, and **row**), pay attention to specific considerations.
- When declaring a decimal type, precision and scale settings are required, and the type definition follows the format `decimal(precision, scale)`. It's essential to emphasize that the declaration of the decimal type must be enclosed in `"`; you cannot use the type name directly, as with basic types. For example, when declaring a decimal field with precision 10 and scale 2, you specify the field type as `"decimal(10,2)"`.
- When declaring an array type, you need to specify the element type, and the type definition follows the format `array<T>`, where `T` represents the element type. The element type includes `int`,`string`,`boolean`,`tinyint`,`smallint`,`bigint`,`float` and `double`. Similar to the decimal type declaration, it also be enclosed in `"`. For example, when declaring a field with an array of integers, you specify the field type as `"array<int>"`.
- When declaring a map type, you need to specify the key and value types. The map type definition follows the format `map<K,V>`, where `K` represents the key type and `V` represents the value type. `K` can be any basic type and decimal type, and `V` can be any type supported by SeaTunnel. Similar to previous type declarations, the map type declaration must be enclosed in double quotes. For example, when declaring a field with map type, where the key type is string and the value type is integer, you can declare the field as `"map<string, int>"`.
- When declaring a row type, you need to define a [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md) object to describe the fields and their types. The field types can be any type supported by SeaTunnel. For example, when declaring a row type containing an integer field `a` and a string field `b`, you can declare it as `{a = int, b = string}`. Enclosing the definition in `"` as a string is also acceptable, so `"{a = int, b = string}"` is equivalent to `{a = int, c = string}`. Since HOCON is compatible with JSON, `"{\"a\":\"int\", \"b\":\"string\"}"` is equivalent to `"{a = int, b = string}"`.

Here is an example of complex type declarations:

```hocon
schema {
  fields {
    c_decimal = "decimal(10, 2)"
    c_array = "array<int>"
    c_row = {
        c_int = int
        c_string = string
        c_row = {
            c_int = int
        }
    }
    # Hocon style declare row type in generic type
    map0 = "map<string, {c_int = int, c_string = string, c_row = {c_int = int}}>"
    # Json style declare row type in generic type
    map1 = "map<string, {\"c_int\":\"int\", \"c_string\":\"string\", \"c_row\":{\"c_int\":\"int\"}}>"
  }
}
```

### PrimaryKey

Primary key is a config used to define the primary key in schema, it contains name, columns field.

```
primaryKey {
    name = id
    columns = [id]
}
```

| Field   | Required | Default Value |            Description            |
|:--------|:---------|:--------------|-----------------------------------|
| name    | Yes      | -             | The name of the primaryKey        |
| columns | Yes      | -             | The column list in the primaryKey |

### ConstraintKeys

Constraint keys is a list of config used to define the constraint keys in schema, it contains constraintName, constraintType, constraintColumns field.

```
constraintKeys = [
      {
         constraintName = "id_index"
         constraintType = KEY
         constraintColumns = [
            {
                columnName = "id"
                sortType = ASC
            }
         ]
      },
   ]
```

| Field             | Required | Default Value |                                                                Description                                                                |
|:------------------|:---------|:--------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| constraintName    | Yes      | -             | The name of the constraintKey                                                                                                             |
| constraintType    | No       | KEY           | The type of the constraintKey                                                                                                             |
| constraintColumns | Yes      | -             | The column list in the primaryKey, each column should contains constraintType and sortType, sortType support ASC and DESC, default is ASC |

#### What constraintType supported at now

| ConstraintType | Description |
|:---------------|:------------|
| INDEX_KEY      | key         |
| UNIQUE_KEY     | unique key  |

## Multi table schemas

```
tables_configs = [
  {
    schema {
      table = "database.schema.table1"
      schema_first = false
      comment = "comment"
      columns = [
        ...
      ]
      primaryKey {
        ...
      }
      constraintKeys {
        ...
      }
    }
  },
  {
    schema = {
      table = "database.schema.table2"
      schema_first = false
      comment = "comment"
      columns = [
        ...
      ]
      primaryKey {
        ...
      }
      constraintKeys {
        ...
      }
    }
  }
]

```

## How to use schema

### Recommended

```
source {
  FakeSource {
    parallelism = 2
    plugin_output = "fake"
    row.num = 16
    schema {
        table = "FakeDatabase.FakeTable"
        columns = [
           {
              name = id
              type = bigint
              nullable = false
              defaultValue = 0
              comment = "primary key id"
           },
           {
              name = name
              type = "string"
              nullable = true
              comment = "name"
           },
           {
              name = age
              type = int
              nullable = true
              comment = "age"
           }
       ]
       primaryKey {
          name = "id"
          columnNames = [id]
       }
       constraintKeys = [
          {
             constraintName = "unique_name"
             constraintType = UNIQUE_KEY
             constraintColumns = [
                {
                    columnName = "name"
                    sortType = ASC
                }
             ]
          },
       ]
      }
    }
}
```

### Deprecated

If you only need to define the column, you can use fields to define the column, this is a simple way but will be remove in the future.

```
source {
  FakeSource {
    parallelism = 2
    plugin_output = "fake"
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
