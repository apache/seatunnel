# Schema 特性简介

## 为什么我们需要Schema

某些NoSQL数据库或消息队列没有严格限制schema，因此无法通过api获取schema。
这时需要定义一个schema来转换为TableSchema并获取数据。

## SchemaOptions

我们可以使用SchemaOptions定义schema, SchemaOptions包含了一些定义schema的配置。 例如：columns, primaryKey, constraintKeys。

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

schema所属的表标识符的表全名，包含数据库、schema、表名。 例如 `database.schema.table`、`database.table`、`table`。

### schema_first

默认是false。

如果schema_first是true, schema会优先使用, 这意味着如果我们设置 `table = "a.b"`, `a` 会被解析为schema而不是数据库, 那么我们可以支持写入 `table = "schema.table"`.

### comment

schema所属的 CatalogTable 的注释。

### Columns

Columns 是用于定义模式中的列的配置列表，每列可以包含名称（name）、类型(type)、是否可空(nullable)、默认值(defaultValue)、注释（comment）字段。

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

| 字段           | 是否必须 | 默认值  |         描述         |
|:-------------|:-----|:-----|--------------------|
| name         | Yes  | -    | 列的名称               |
| type         | Yes  | -    | 列的数据类型             |
| nullable     | No   | true | 列是否可空              |
| columnLength | No   | 0    | 列的长度，当您需要定义长度时将很有用 |
| columnScale  | No   | -    | 列的精度，当您需要定义精度时将很有用 |
| defaultValue | No   | null | 列的默认值              |
| comment      | No   | null | 列的注释               |

#### 目前支持哪些类型

| 数据类型         | Java中的值类型                                          | 描述                                                                                                                                                                                                                                                                                                              |
|:-------------|:---------------------------------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| string       | `java.lang.String`                                 | 字符串                                                                                                                                                                                                                                                                                                             |
| boolean      | `java.lang.Boolean`                                | 布尔                                                                                                                                                                                                                                                                                                              |
| tinyint      | `java.lang.Byte`                                   | 常规-128 至 127 。 0 到 255 无符号*。 指定括号中的最大位数。                                                                                                                                                                                                                                                                        |
| smallint     | `java.lang.Short`                                  | 常规-32768 至 32767。 0 到 65535 无符号*。 指定括号中的最大位数。                                                                                                                                                                                                                                                                   |
| int          | `java.lang.Integer`                                | 允许从 -2,147,483,648 到 2,147,483,647 的所有数字。                                                                                                                                                                                                                                                                       |
| bigint       | `java.lang.Long`                                   | 允许 -9,223,372,036,854,775,808 和 9,223,372,036,854,775,807 之间的所有数字。                                                                                                                                                                                                                                              |
| float        | `java.lang.Float`                                  | 从-1.79E+308 到 1.79E+308浮点精度数值数据。                                                                                                                                                                                                                                                                                |
| double       | `java.lang.Double`                                 | 双精度浮点。 处理大多数小数。                                                                                                                                                                                                                                                                                                 |
| decimal      | `java.math.BigDecimal`                             | Double 类型存储为字符串，允许固定小数点。                                                                                                                                                                                                                                                                                        |
| null         | `java.lang.Void`                                   | null                                                                                                                                                                                                                                                                                                            |
| bytes        | `byte[]`                                           | 字节。                                                                                                                                                                                                                                                                                                             |
| date         | `java.time.LocalDate`                              | 仅存储日期。从0001年1月1日到9999 年 12 月 31 日。                                                                                                                                                                                                                                                                              |
| time         | `java.time.LocalTime`                              | 仅存储时间。精度为 100 纳秒。                                                                                                                                                                                                                                                                                               |
| timestamp    | `java.time.LocalDateTime`                          | 存储不带时区的日期和时间信息，表示事件发生的本地时间。不包含任何偏移量或时区相关信息。                                                                                                                                           |
| timestamp_tz | `java.time.OffsetDateTime`                         | 存储带有 UTC 偏移量的日期和时间信息，包含本地日期时间和 UTC 偏移量。在处理多时区场景时，可以提供更精确的时间信息。                                                                                     |
| row          | `org.apache.seatunnel.api.table.type.SeaTunnelRow` | 行类型，可以嵌套。                                                                                                                                                                                                                                                                                                       |
| map          | `java.util.Map`                                    | Map 是将键映射到值的对象。 键类型包括： `int` `string` `boolean` `tinyint` `smallint` `bigint` `float` `double` `decimal` `date` `time` `timestamp` `null` , and the value type includes `int` `string` `boolean` `tinyint` `smallint` `bigint` `float` `double` `decimal` `date` `time` `timestamp` `null` `array` `map` `row`. |
| array        | `ValueType[]`                                      | 数组是一种表示元素集合的数据类型。 元素类型包括： `int` `string` `boolean` `tinyint` `smallint` `bigint` `float` `double`.                                                                                                                                                                                                              |

#### 如何声明支持的类型

SeaTunnel 提供了一种简单直接的方式来声明基本类型。基本类型的关键字包括：`string`, `boolean`, `tinyint`, `smallint`, `int`, `bigint`, `float`, `double`, `date`, `time`, `timestamp`, 和 `null`。基本类型的关键字名称可以直接用作类型声明，并且SeaTunnel对类型关键字不区分大小写。 例如，如果您需要声明一个整数类型的字段，您可以简单地将字段定义为`int`或`"int"`。

> null 类型声明必须用双引号引起来, 例如：`"null"`。 这种方法有助于避免与 [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md) 中表示未定义的对象的 `null` 类型混淆。

声明复杂类型（例如 **decimal**、**array**、**map** 和 **row**）时，请注意具体注意事项。
- 声明decimal类型时，需要设置精度(precision)和小数位数(scale)，类型定义遵循“decimal(precision, scale)”格式。 需要强调的是，十进制类型的声明必须用 `"` 括起来；不能像基本类型一样直接使用类型名称。例如，当声明精度为 10、小数位数为 2 的十进制字段时，您可以指定字段类型为`"decimal(10,2)"`。
- 声明array类型时，需要指定元素类型，类型定义遵循 `array<T>` 格式，其中 `T` 代表元素类型。元素类型包括`int`,`string`,`boolean`,`tinyint`,`smallint`,`bigint`,`float` 和 `double`。与十进制类型声明类似，它也用 `"` 括起来。例如，在声明具有整数数组的字段时，将字段类型指定为 `"array<int>"`。
- 声明map类型时，需要指定键和值类型。map类型定义遵循`map<K,V>`格式，其中`K`表示键类型，`V`表示值类型。 `K`可以是任何基本类型和十进制类型，`V`可以是 SeaTunnel 支持的任何类型。 与之前的类型声明类似，map类型声明必须用双引号引起来。 例如，当声明一个map类型的字段时，键类型为字符串，值类型为整数，则可以将该字段声明为`"map<string, int>"`。
- 声明row类型时，需要定义一个 [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md) 对象来描述字段及其类型。 字段类型可以是 SeaTunnel 支持的任何类型。 例如，当声明包含整数字段“a”和字符串字段“b”的行类型时，可以将其声明为“{a = int, b = string}”。 将定义作为字符串括在 `"` 中也是可以接受的，因此 `"{a = int, b = string}"` 相当于 `{a = int, c = string}`。由于 HOCON 与 JSON 兼容， `"{\"a\":\"int\", \"b\":\"string\"}"` 等价于 `"{a = int, b = string}"`。

以下是复杂类型声明的示例：

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
    # 在泛型中Hocon风格声明行类型
    map0 = "map<string, {c_int = int, c_string = string, c_row = {c_int = int}}>"
    # 在泛型中Json风格声明行类型
    map1 = "map<string, {\"c_int\":\"int\", \"c_string\":\"string\", \"c_row\":{\"c_int\":\"int\"}}>"
  }
}
```

### 主键（PrimaryKey）

主键是用于定义模式中主键的配置，它包含name、columns字段。

```
primaryKey {
    name = id
    columns = [id]
}
```

| 字段      | 是否必须 | 默认值 |   描述    |
|:--------|:-----|:----|---------|
| name    | 是    | -   | 主键名称    |
| columns | 是    | -   | 主键中的列列表 |

### 约束键（constraintKeys）

约束键是用于定义模式中约束键的配置列表，它包含constraintName，constraintType，constraintColumns字段。

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

| 字段                | 是否必须 | 默认值 |                                   描述                                   |
|:------------------|:-----|:----|------------------------------------------------------------------------|
| constraintName    | 是    | -   | 约束键的名称                                                                 |
| constraintType    | 否    | KEY | 约束键的类型                                                                 |
| constraintColumns | 是    | -   | PrimaryKey中的列列表，每列应包含constraintType和sortType，sortType支持ASC和DESC，默认为ASC |

#### 目前支持哪些约束类型

| 约束类型       | 描述  |
|:-----------|:----|
| INDEX_KEY  | 键   |
| UNIQUE_KEY | 唯一键 |

## 多表Schema

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

## 如何使用schema

### 推荐

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

### 已弃用

如果你只需要定义列，你可以使用字段来定义列，这是一种简单的方式，但将来会被删除。

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

## 我们什么时候应该使用它，什么时候不应该使用它

如果选项中有`schema`配置项目，则连接器可以自定义schema。 比如 `Fake` `Pulsar` `Http` 源连接器等。
