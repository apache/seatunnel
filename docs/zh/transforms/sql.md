# SQL

> SQL 转换插件

## 描述

使用 SQL 来转换给定的输入行。

SQL 转换使用内存中的 SQL 引擎，我们可以通过 SQL 函数和 SQL 引擎的能力来实现转换任务。

## 属性

|        名称         |   类型   | 是否必须 | 默认值 |
|-------------------|--------|------|-----|
| plugin_input | string | yes  | -   |
| plugin_output | string | yes  | -   |
| query             | string | yes  | -   |
| engine            | string | no   | ZETA |

### plugin_input [string]

源表名称，查询 SQL 表名称必须与此字段匹配。

### query [string]

查询 SQL，它是一个简单的 SQL，支持基本的函数和条件过滤操作。但是，复杂的 SQL 尚不支持，包括：多源表/行连接和聚合操作等。

查询表达式可以是`select [table_name.]column_a`，这时会去查询列为`column_a`的列，`table_name`为可选项
也可以是`select c_row.c_inner_row.column_b`，这时会去查询列`c_row`下的`c_inner_row`的`column_b`。**嵌套结构查询中，不能存在`table_name`**

### engine [string]

该 Transform 使用的 SQL 引擎。支持 `ZETA` 和 `INTERNAL`。如果不配置，默认使用 `ZETA`。

## 数值比较

当求值后的两个操作数均为整型（`TINYINT`、`SMALLINT`、`INT`、`BIGINT`）或 `DECIMAL` 时，比较保留数值精度。
此规则适用于 `=`、`!=`、`<>`、`<`、`<=`、`>`、`>=`、数值的 `IN` 和 `NOT IN` 判断，
以及搜索式和简单式 `CASE` 表达式中的比较。
例如，`BIGINT` 值 `9007199254740993` 不会匹配 `WHERE id = 9007199254740992`。

当任一操作数为 `FLOAT` 或 `DOUBLE` 时，仍使用浮点转换进行比较，保留 NaN、无穷大和带符号零的原有行为。
包含小数点的数值字面量求值为 `DOUBLE`，而非 `DECIMAL`：因此，将 `DECIMAL` 列与
`123456789012345678.98` 比较时，仍可能匹配 `123456789012345678.99`。
求值为整型的整数字面量（如 `-9007199254740993`）在另一操作数也是精确类型时使用精确比较。
对 `DECIMAL` 表达式应用一元负号目前会产生 `DOUBLE`，因此 `-decimal_col` 和
`-CAST('...' AS DECIMAL(p, s))` 不会保留精确的小数比较语义。

如需精确的小数常量，请使用带引号的字符串并保留足够的小数位，例如
`amount = CAST('123456789012345678.99' AS DECIMAL(38, 2))`。
负数常量的符号应放在字符串内：`CAST('-123456789012345678.99' AS DECIMAL(38, 2))`。
现有 CAST 舍入行为不变：将字符串或 `DECIMAL` 转换为 `DECIMAL(p, s)` 时，
如果减少小数位，则采用 `CEILING`（向正无穷方向舍入）。因此，
`CAST('1.231' AS DECIMAL(10, 2))` 为 `1.24`，
`CAST('-1.239' AS DECIMAL(10, 2))` 为 `-1.23`。
转换 `FLOAT` 或 `DOUBLE` 时采用 `HALF_UP`；对带小数点的数值字面量进行 CAST，
无法恢复其求值为 `DOUBLE` 时已丢失的精度。

每一对操作数独立选择精确或浮点比较，包括 `IN` 列表中的每个元素。
当 `id = 9007199254740993` 时，`id IN (9007199254740992)` 为假，
但 `id IN (9007199254740992, 9007199254740992.0)` 为真，因为第二次比较使用 `DOUBLE`。
因此，在精确类型与浮点类型混用时，等号不一定满足传递性；需要精度保证时，请统一使用精确类型的操作数。
以上规则适用于 `ZETA` 和 `INTERNAL` 两种 SQL 引擎设置。

## 示例

源端数据读取的表格如下：

| id |   name   | age |
|----|----------|-----|
| 1  | Joy Ding | 20  |
| 2  | May Ding | 21  |
| 3  | Kin Dom  | 24  |
| 4  | Joy Dom  | 22  |

我们使用 SQL 查询来转换源数据，类似这样：

```
transform {
  Sql {
    plugin_input = "fake"
    plugin_output = "fake1"
    query = "select id, concat(name, '_') as name, age+1 as age from dual where id>0"
  }
}
```

那么结果表 `fake1` 中的数据将会更新为：

| id |   name    | age |
|----|-----------|-----|
| 1  | Joy Ding_ | 21  |
| 2  | May Ding_ | 22  |
| 3  | Kin Dom_  | 25  |
| 4  | Joy Dom_  | 23  |

### 嵌套结构查询

例如你的上游数据结构是这样：

```hacon
source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    string.template = ["innerQuery"]
    schema = {
      fields {
        name = "string"
        c_date = "date"
        c_row = {
          c_inner_row = {
            c_inner_int = "int"
            c_inner_string = "string"
            c_inner_timestamp = "timestamp"
            c_map_1 = "map<string, string>"
            c_map_2 = "map<string, map<string,string>>"
          }
          c_string = "string"
        }
      }
    }
  }
}
```

那么下列所有的查询表达式都是有效的

```sql
select 
name,
c_date,
c_row,
c_row.c_inner_row,
c_row.c_string,
c_row.c_inner_row.c_inner_int,
c_row.c_inner_row.c_inner_string,
c_row.c_inner_row.c_inner_timestamp,
c_row.c_inner_row.c_map_1,
c_row.c_inner_row.c_map_1.some_key
```

但是这个查询语句是无效的

```sql
select 
c_row.c_inner_row.c_map_2.some_key.inner_map_key
```

当查询map结构时，map结构应该为最后一个数据结构，不能查询嵌套map

## 作业配置示例

```
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
      }
    }
  }
}

transform {
  Sql {
    plugin_input = "fake"
    plugin_output = "fake1"
    query = "select id, concat(name, '_') as name, age+1 as age from dual where id>0"
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

## 更新日志

### 新版本

- 添加SQL转换连接器
