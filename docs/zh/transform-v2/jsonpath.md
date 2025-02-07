# JsonPath

> JSONPath 转换插件

## 描述

> 支持使用 JSONPath 选择数据

## 属性

| 名称                   | 类型    | 是否必须 | 默认值  |
|----------------------|-------|------|------|    
| columns              | Array | Yes  |      | 
| row_error_handle_way | Enum  | No   | FAIL |

### common options [string]

转换插件的常见参数, 请参考  [Transform Plugin](common-options.md) 了解详情

### row_error_handle_way [Enum]

该选项用于指定当该行发生错误时的处理方式，默认值为 `FAIL`。

- FAIL：选择`FAIL`时，数据格式错误会阻塞并抛出异常。
- SKIP：选择`SKIP`时，数据格式错误会跳过该行数据。

### columns[array]

#### 属性

| 名称                      | 类型     | 是否必须 | 默认值    |
|-------------------------|--------|------|--------|
| src_field               | String | Yes  |        |
| dest_field              | String | Yes  |        |
| path                    | String | Yes  |        |
| dest_type               | String | No   | String |
| column_error_handle_way | Enum   | No   |        |

#### src_field

> 要解析的 JSON 源字段

支持的Seatunnel数据类型

* STRING
* BYTES
* ARRAY
* MAP
* ROW

#### dest_field

> 使用 JSONPath 后的输出字段

#### dest_type

> 目标字段的类型

#### path

> Jsonpath

#### column_error_handle_way [Enum]

该选项用于指定当列发生错误时的处理方式。

- FAIL：选择`FAIL`时，数据格式错误会阻塞并抛出异常。
- SKIP：选择`SKIP`时，数据格式错误会跳过此列数据。
- SKIP_ROW：选择`SKIP_ROW`时，数据格式错误会跳过此行数据。

## 读取 JSON 示例

从源读取的数据是像这样的 JSON

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

假设我们想要使用 JsonPath 提取属性。

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

那么数据结果表 `fake1` 将会像这样

|             data             |    c1_string     | c1_boolean | c1_integer | c1_float | c1_double | c1_decimal |  c1_date   | c1_datetime  |          c1_array           |
|------------------------------|------------------|------------|------------|----------|-----------|------------|------------|--------------|-----------------------------|
| too much content not to show | this is a string | true       | 42         | 3.14     | 3.14      | 10.55      | 2023-10-29 | 16:12:43.459 | ["item1", "item2", "item3"] |

## 读取 SeatunnelRow 示例

假设数据行中的一列的类型是 SeatunnelRow，列的名称为 col

<table>
<tr><th colspan="2">SeatunnelRow(col)</th><th>other</th></tr>
<tr><td>name</td><td>age</td><td>....</td></tr>
<tr><td>a</td><td>18</td><td>....</td></tr>
</table>

JsonPath 转换将 seatunnel 的值转换为一个数组。

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

那么数据结果表 `fake1` 将会像这样:

| name | age |   col    | other |
|------|-----|----------|-------|
| a    | 18  | ["a",18] | ...   |

## 配置异常数据处理策略

您可以配置 `row_error_handle_way` 与 `column_error_handle_way` 来处理异常数据，两者都是非必填项。

`row_error_handle_way` 配置对行数据内所有数据异常进行处理，`column_error_handle_way` 配置对某列数据异常进行处理，优先级高于 `row_error_handle_way`。

### 跳过异常数据行

配置跳过任意列有异常的整行数据

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

### 跳过部分异常数据列

配置仅对 `json_data_f1` 列数据异常跳过，填充空值，其他列数据异常继续抛出异常中断处理程序

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

### 部分列异常跳过整行

配置仅对 `json_data_f1` 列数据异常跳过整行数据，其他列数据异常继续抛出异常中断处理程序

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

## 更新日志

* 添加 JsonPath 转换

