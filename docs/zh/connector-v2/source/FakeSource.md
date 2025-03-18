import ChangeLog from '../changelog/connector-fake.md';

# FakeSource

> FakeSource 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

FakeSource 是一个虚拟数据源，它根据用户定义的 schema 数据结构随机生成指定数量的行数据，主要用于类型转换或连接器新功能测试等测试场景。

## 主要特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [x] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)
- [ ] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../concept/connector-v2-features.md)

## 数据源选项

| 名称                      | 类型    | 必填 | 默认值  | 描述                                                                                                                                                                                                 |
|---------------------------|---------|------|---------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| tables_configs            | list    | 否   | -       | 定义多个 FakeSource，每个项可以包含完整的 FakeSource 配置描述                                                                                                                                         |
| schema                    | config    | 是   | -       | 定义 Schema 信息                                                                                                                                                                                     |
| rows                      | config    | 否   | -       | 每个并行度输出的伪数据行列表，详见标题 `Options rows Case`                                                                                                                                            |
| row.num                   | int    | 否   | 5       | 每个并行度生成的数据总行数                                                                                                                                                                           |
| split.num                 | int    | 否   | 1       | 枚举器为每个并行度生成的分片数量                                                                                                                                                                     |
| split.read-interval       | long  | 否   | 1       | 读取器在两个分片读取之间的间隔时间（毫秒）                                                                                                                                                           |
| map.size                  | int    | 否   | 5       | 连接器生成的 `map` 类型的大小                                                                                                                                                                        |
| array.size                | int    | 否   | 5       | 连接器生成的 `array` 类型的大小                                                                                                                                                                      |
| bytes.length              | int    | 否   | 5       | 连接器生成的 `bytes` 类型的长度                                                                                                                                                                      |
| string.length             | int    | 否   | 5       | 连接器生成的 `string` 类型的长度                                                                                                                                                                     |
| string.fake.mode          | string  | 否   | range   | 生成字符串数据的伪数据模式，支持 `range` 和 `template`，默认为 `range`，如果配置为 `template`，用户还需配置 `string.template` 选项                                                                   |
| string.template           | list    | 否   | -       | 连接器生成的字符串类型的模板列表，如果用户配置了此选项，连接器将从模板列表中随机选择一个项                                                                                                             |
| tinyint.fake.mode         | string  | 否   | range   | 生成 tinyint 数据的伪数据模式，支持 `range` 和 `template`，默认为 `range`，如果配置为 `template`，用户还需配置 `tinyint.template` 选项                                                               |
| tinyint.min               | tinyint | 否   | 0       | 连接器生成的 tinyint 数据的最小值                                                                                                                                                                    |
| tinyint.max               | tinyint | 否   | 127     | 连接器生成的 tinyint 数据的最大值                                                                                                                                                                    |
| tinyint.template          | list    | 否   | -       | 连接器生成的 tinyint 类型的模板列表，如果用户配置了此选项，连接器将从模板列表中随机选择一个项                                                                                                         |
| smallint.fake.mode        | string  | 否   | range   | 生成 smallint 数据的伪数据模式，支持 `range` 和 `template`，默认为 `range`，如果配置为 `template`，用户还需配置 `smallint.template` 选项                                                             |
| smallint.min              | smallint| 否   | 0       | 连接器生成的 smallint 数据的最小值                                                                                                                                                                   |
| smallint.max              | smallint| 否   | 32767   | 连接器生成的 smallint 数据的最大值                                                                                                                                                                   |
| smallint.template         | list    | 否   | -       | 连接器生成的 smallint 类型的模板列表，如果用户配置了此选项，连接器将从模板列表中随机选择一个项                                                                                                       |
| int.fake.template         | string  | 否   | range   | 生成 int 数据的伪数据模式，支持 `range` 和 `template`，默认为 `range`，如果配置为 `template`，用户还需配置 `int.template` 选项                                                                       |
| int.min                   | smallint    | 否   | 0       | 连接器生成的 int 数据的最小值                                                                                                                                                                        |
| int.max                   | smallint    | 否   | 0x7fffffff | 连接器生成的 int 数据的最大值                                                                                                                                                                        |
| int.template              | list    | 否   | -       | 连接器生成的 int 类型的模板列表，如果用户配置了此选项，连接器将从模板列表中随机选择一个项                                                                                                             |
| bigint.fake.mode          | string  | 否   | range   | 生成 bigint 数据的伪数据模式，支持 `range` 和 `template`，默认为 `range`，如果配置为 `template`，用户还需配置 `bigint.template` 选项                                                                 |
| bigint.min                | bigint  | 否   | 0       | 连接器生成的 bigint 数据的最小值                                                                                                                                                                     |
| bigint.max                | bigint  | 否   | 0x7fffffffffffffff | 连接器生成的 bigint 数据的最大值                                                                                                                                                                     |
| bigint.template           | list    | 否   | -       | 连接器生成的 bigint 类型的模板列表，如果用户配置了此选项，连接器将从模板列表中随机选择一个项                                                                                                         |
| float.fake.mode           | string  | 否   | range   | 生成 float 数据的伪数据模式，支持 `range` 和 `template`，默认为 `range`，如果配置为 `template`，用户还需配置 `float.template` 选项                                                                   |
| float.min                 | float   | 否   | 0       | 连接器生成的 float 数据的最小值                                                                                                                                                                      |
| float.max                 | float   | 否   | 0x1.fffffeP+127 | 连接器生成的 float 数据的最大值                                                                                                                                                                      |
| float.template            | list    | 否   | -       | 连接器生成的 float 类型的模板列表，如果用户配置了此选项，连接器将从模板列表中随机选择一个项                                                                                                           |
| double.fake.mode          | string  | 否   | range   | 生成 double 数据的伪数据模式，支持 `range` 和 `template`，默认为 `range`，如果配置为 `template`，用户还需配置 `double.template` 选项                                                                 |
| double.min                | double  | 否   | 0       | 连接器生成的 double 数据的最小值                                                                                                                                                                     |
| double.max                | double  | 否   | 0x1.fffffffffffffP+1023 | 连接器生成的 double 数据的最大值                                                                                                                                                                     |
| double.template           | list    | 否   | -       | 连接器生成的 double 类型的模板列表，如果用户配置了此选项，连接器将从模板列表中随机选择一个项                                                                                                         |
| vector.dimension          | int    | 否   | 4       | 生成的向量的维度，不包括二进制向量                                                                                                                                                                   |
| binary.vector.dimension   | int    | 否   | 8       | 生成的二进制向量的维度                                                                                                                                                                               |
| vector.float.min          | float   | 否   | 0       | 连接器生成的向量中 float 数据的最小值                                                                                                                                                                |
| vector.float.max          | float   | 否   | 0x1.fffffeP+127 | 连接器生成的向量中 float 数据的最大值                                                                                                                                                                |
| common-options            |         | 否   | -       | 数据源插件通用参数，详情请参考 [Source Common Options](../source-common-options.md)                                                                                                                 |

## 任务示例

### 简单示例：

> 此示例随机生成指定类型的数据。如果您想了解如何声明字段类型，请点击 [这里](../../concept/schema-feature.md#how-to-declare-type-supported)。

```hocon
schema = {
  fields {
    c_map = "map<string, array<int>>"
    c_map_nest = "map<string, {c_int = int, c_string = string}>"
    c_array = "array<int>"
    c_string = string
    c_boolean = boolean
    c_tinyint = tinyint
    c_smallint = smallint
    c_int = int
    c_bigint = bigint
    c_float = float
    c_double = double
    c_decimal = "decimal(30, 8)"
    c_null = "null"
    c_bytes = bytes
    c_date = date
    c_timestamp = timestamp
    c_row = {
      c_map = "map<string, map<string, string>>"
      c_array = "array<int>"
      c_string = string
      c_boolean = boolean
      c_tinyint = tinyint
      c_smallint = smallint
      c_int = int
      c_bigint = bigint
      c_float = float
      c_double = double
      c_decimal = "decimal(30, 8)"
      c_null = "null"
      c_bytes = bytes
      c_date = date
      c_timestamp = timestamp
    }
  }
}
```

### 随机生成

> 随机生成 16 条符合类型的数据

```hocon
source {
  # 这是一个示例输入插件，**仅用于测试和演示功能输入插件**
  FakeSource {
    row.num = 16
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
    plugin_output = "fake"
  }
}
```

### 自定义数据内容简单示例：

> 这是一个自定义数据源信息的示例，定义每条数据是添加还是删除修改操作，并定义每个字段存储的内容

```hocon
source {
  FakeSource {
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [{"a": "b"}, [101], "c_string", true, 117, 15987, 56387395, 7084913402530365000, 1.23, 1.23, "2924137191386439303744.39292216", null, "bWlJWmo=", "2023-04-22", "2023-04-22T23:20:58"]
      }
      {
        kind = UPDATE_BEFORE
        fields = [{"a": "c"}, [102], "c_string", true, 117, 15987, 56387395, 7084913402530365000, 1.23, 1.23, "2924137191386439303744.39292216", null, "bWlJWmo=", "2023-04-22", "2023-04-22T23:20:58"]
      }
      {
        kind = UPDATE_AFTER
        fields = [{"a": "e"}, [103], "c_string", true, 117, 15987, 56387395, 7084913402530365000, 1.23, 1.23, "2924137191386439303744.39292216", null, "bWlJWmo=", "2023-04-22", "2023-04-22T23:20:58"]
      }
      {
        kind = DELETE
        fields = [{"a": "f"}, [104], "c_string", true, 117, 15987, 56387395, 7084913402530365000, 1.23, 1.23, "2924137191386439303744.39292216", null, "bWlJWmo=", "2023-04-22", "2023-04-22T23:20:58"]
      }
    ]
  }
}
```

> 由于 [HOCON](https://github.com/lightbend/config/blob/main/HOCON.md) 规范的限制，用户无法直接创建字节序列对象。FakeSource 使用字符串来分配 `bytes` 类型的值。在上面的示例中，`bytes` 类型字段被分配了 `"bWlJWmo="`，这是通过 **base64** 编码的 "miIZj"。因此，在为 `bytes` 类型字段赋值时，请使用 **base64** 编码的字符串。

### 指定数据数量简单示例：

> 此案例指定生成数据的数量以及生成值的长度

```hocon
FakeSource {
  row.num = 10
  map.size = 10
  array.size = 10
  bytes.length = 10
  string.length = 10
  schema = {
    fields {
      c_map = "map<string, array<int>>"
      c_array = "array<int>"
      c_string = string
      c_boolean = boolean
      c_tinyint = tinyint
      c_smallint = smallint
      c_int = int
      c_bigint = bigint
      c_float = float
      c_double = double
      c_decimal = "decimal(30, 8)"
      c_null = "null"
      c_bytes = bytes
      c_date = date
      c_timestamp = timestamp
      c_row = {
        c_map = "map<string, map<string, string>>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
  }
}
```

### 模板数据简单示例：

> 根据指定模板随机生成

使用模板

```hocon
FakeSource {
  row.num = 5
  string.fake.mode = "template"
  string.template = ["tyrantlucifer", "hailin", "kris", "fanjia", "zongwen", "gaojun"]
  tinyint.fake.mode = "template"
  tinyint.template = [1, 2, 3, 4, 5, 6, 7, 8, 9]
  smalling.fake.mode = "template"
  smallint.template = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
  int.fake.mode = "template"
  int.template = [20, 21, 22, 23, 24, 25, 26, 27, 28, 29]
  bigint.fake.mode = "template"
  bigint.template = [30, 31, 32, 33, 34, 35, 36, 37, 38, 39]
  float.fake.mode = "template"
  float.template = [40.0, 41.0, 42.0, 43.0]
  double.fake.mode = "template"
  double.template = [44.0, 45.0, 46.0, 47.0]
  schema {
    fields {
      c_string = string
      c_tinyint = tinyint
      c_smallint = smallint
      c_int = int
      c_bigint = bigint
      c_float = float
      c_double = double
    }
  }
}
```

### 范围数据简单示例：

> 在指定的数据生成范围内随机生成

```hocon
FakeSource {
  row.num = 5
  string.template = ["tyrantlucifer", "hailin", "kris", "fanjia", "zongwen", "gaojun"]
  tinyint.min = 1
  tinyint.max = 9
  smallint.min = 10
  smallint.max = 19
  int.min = 20
  int.max = 29
  bigint.min = 30
  bigint.max = 39
  float.min = 40.0
  float.max = 43.0
  double.min = 44.0
  double.max = 47.0
  schema {
    fields {
      c_string = string
      c_tinyint = tinyint
      c_smallint = smallint
      c_int = int
      c_bigint = bigint
      c_float = float
      c_double = double
    }
  }
}
```


### 生成多张表

> 这是一个生成多数据源测试表 `test.table1` 和 `test.table2` 的示例

```hocon
FakeSource {
  tables_configs = [
    {
      row.num = 16
      schema {
        table = "test.table1"
        fields {
          c_string = string
          c_tinyint = tinyint
          c_smallint = smallint
          c_int = int
          c_bigint = bigint
          c_float = float
          c_double = double
        }
      }
    },
    {
      row.num = 17
      schema {
        table = "test.table2"
        fields {
          c_string = string
          c_tinyint = tinyint
          c_smallint = smallint
          c_int = int
          c_bigint = bigint
          c_float = float
          c_double = double
        }
      }
    }
  ]
}
```

### `rows` 选项示例

```hocon
rows = [
  {
    kind = INSERT
    fields = [1, "A", 100]
  },
  {
    kind = UPDATE_BEFORE
    fields = [1, "A", 100]
  },
  {
    kind = UPDATE_AFTER
    fields = [1, "A_1", 100]
  },
  {
    kind = DELETE
    fields = [1, "A_1", 100]
  }
]
```

### `table-names` 选项示例

```hocon
source {
  # 这是一个示例源插件，**仅用于测试和演示源插件功能**
  FakeSource {
    table-names = ["test.table1", "test.table2", "test.table3"]
    parallelism = 1
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}
```

### `defaultValue` 选项示例

可以通过 `row` 和 `columns` 生成自定义数据。对于时间类型，可以通过 `CURRENT_TIMESTAMP`、`CURRENT_TIME`、`CURRENT_DATE` 获取当前时间。

```hocon
    schema = {
        fields {
            pk_id = bigint
            name = string
            score = int
            time1 = timestamp
            time2 = time
            time3 = date
        }
    }
    # 使用 rows
    rows = [
        {
            kind = INSERT
            fields = [1, "A", 100, CURRENT_TIMESTAMP, CURRENT_TIME, CURRENT_DATE]
        }
    ]
```

```hocon
      schema = {
          # 使用 columns
           columns = [
           {
              name = book_publication_time
              type = timestamp
              defaultValue = "2024-09-12 15:45:30"
              comment = "书籍出版时间"
           },
           {
              name = book_publication_time2
              type = timestamp
              defaultValue = CURRENT_TIMESTAMP
              comment = "书籍出版时间2"
           },
           {
              name = book_publication_time3
              type = time
              defaultValue = "15:45:30"
              comment = "书籍出版时间3"
           },
           {
              name = book_publication_time4
              type = time
              defaultValue = CURRENT_TIME
              comment = "书籍出版时间4"
           },
           {
              name = book_publication_time5
              type = date
              defaultValue = "2024-09-12"
              comment = "书籍出版时间5"
           },
           {
              name = book_publication_time6
              type = date
              defaultValue = CURRENT_DATE
              comment = "书籍出版时间6"
           }
       ]
      }
```

### 使用向量示例

```hocon
source {
  FakeSource {
      row.num = 10
      # 低优先级 
      vector.dimension= 4
      binary.vector.dimension = 8
      # 低优先级 
      schema = {
           table = "simple_example"
           columns = [
           {
              name = book_id
              type = bigint
              nullable = false
              defaultValue = 0
              comment = "主键 ID"
           },
            {
              name = book_intro_1
              type = binary_vector
              columnScale =8
              comment = "向量"
           },
           {
              name = book_intro_2
              type = float16_vector
              columnScale =4
              comment = "向量"
           },
           {
              name = book_intro_3
              type = bfloat16_vector
              columnScale =4
              comment = "向量"
           },
           {
              name = book_intro_4
              type = sparse_float_vector
              columnScale =4
              comment = "向量"
           }
       ]
     }
  }
}
```

## 变更日志

<ChangeLog />