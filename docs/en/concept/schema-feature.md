# Intro to schema feature

## Why we need schema

Some NoSQL databases or message queue are not strongly limited schema, so the schema cannot be obtained through the api.
At this time, a schema needs to be defined to convert to TableSchema and obtain data.

## SchemaOptions

We can use SchemaOptions to define schema, the SchemaOptions contains some config to define the schema. e.g. columns, primaryKey, constraintKeys.

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

Columns is a list of config used to define the column in schema, each column can contains name, type, nullable, defaultValue, comment field.

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
| defaultValue | No       | null          | The default value of the column                                                  |
| comment      | No       | null          | The comment of the column                                                        |

#### What type supported at now

| Data type | Value type in Java                                 | Description                                                                                                                                                                                                                                                                                                                                           |
|:----------|:---------------------------------------------------|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| string    | `java.lang.String`                                 | string                                                                                                                                                                                                                                                                                                                                                |
| boolean   | `java.lang.Boolean`                                | boolean                                                                                                                                                                                                                                                                                                                                               |
| tinyint   | `java.lang.Byte`                                   | -128 to 127 regular. 0 to 255 unsigned*. Specify the maximum number of digits in parentheses.                                                                                                                                                                                                                                                         |
| smallint  | `java.lang.Short`                                  | -32768 to 32767 General. 0 to 65535 unsigned*. Specify the maximum number of digits in parentheses.                                                                                                                                                                                                                                                   |
| int       | `java.lang.Integer`                                | All numbers from -2,147,483,648 to 2,147,483,647 are allowed.                                                                                                                                                                                                                                                                                         |
| bigint    | `java.lang.Long`                                   | All numbers between -9,223,372,036,854,775,808 and 9,223,372,036,854,775,807 are allowed.                                                                                                                                                                                                                                                             |
| float     | `java.lang.Float`                                  | Float-precision numeric data from -1.79E+308 to 1.79E+308.                                                                                                                                                                                                                                                                                            |
| double    | `java.lang.Double`                                 | Double precision floating point. Handle most decimals.                                                                                                                                                                                                                                                                                                |
| decimal   | `java.math.BigDecimal`                             | DOUBLE type stored as a string, allowing a fixed decimal point.                                                                                                                                                                                                                                                                                       |
| null      | `java.lang.Void`                                   | null                                                                                                                                                                                                                                                                                                                                                  |
| bytes     | `byte[]`                                           | bytes.                                                                                                                                                                                                                                                                                                                                                |
| date      | `java.time.LocalDate`                              | Only the date is stored. From January 1, 0001 to December 31, 9999.                                                                                                                                                                                                                                                                                   |
| time      | `java.time.LocalTime`                              | Only store time. Accuracy is 100 nanoseconds.                                                                                                                                                                                                                                                                                                         |
| timestamp | `java.time.LocalDateTime`                          | Stores a unique number that is updated whenever a row is created or modified. timestamp is based on the internal clock and does not correspond to real time. There can only be one timestamp variable per table.                                                                                                                                      |
| row       | `org.apache.seatunnel.api.table.type.SeaTunnelRow` | Row type,can be nested.                                                                                                                                                                                                                                                                                                                               |
| map       | `java.util.Map`                                    | A Map is an object that maps keys to values. The key type includes `int` `string` `boolean` `tinyint` `smallint` `bigint` `float` `double` `decimal` `date` `time` `timestamp` `null` , and the value type includes `int` `string` `boolean` `tinyint` `smallint` `bigint` `float` `double` `decimal` `date` `time` `timestamp` `null` `array` `map`. |
| array     | `ValueType[]`                                      | A array is a data type that represents a collection of elements. The element type includes `int` `string` `boolean` `tinyint` `smallint` `bigint` `float` `double` `array` `map`.                                                                                                                                                                     |

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

## How to use schema

### Recommended

```
source {
  FakeSource {
    parallelism = 2
    result_table_name = "fake"
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
