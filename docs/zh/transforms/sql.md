# SQL

> SQL 转换插件

## 描述

使用 SQL 来转换给定的输入行。

SQL 转换使用内存中的 SQL 引擎，我们可以通过 SQL 函数和 SQL 引擎的能力来实现转换任务。

## 属性

|        名称         |   类型   | 是否必须 | 默认值 |
|-------------------|--------|------|-----|
| plugin_input | string | no   | -   |
| plugin_output | string | no   | -   |
| query             | string | yes  | -   |
| engine            | string | no   | ZETA |
| where_rejected_update_as_delete | boolean | no | false |

### plugin_input [string]

源表名称，查询 SQL 表名称必须与此字段匹配。如果不配置，将使用输入目录表的表名。

### query [string]

查询 SQL，它是一个简单的 SQL，支持基本的函数和条件过滤操作。但是，复杂的 SQL 尚不支持，包括：多源表/行连接和聚合操作等。

查询表达式可以是`select [table_name.]column_a`，这时会去查询列为`column_a`的列，`table_name`为可选项
也可以是`select c_row.c_inner_row.column_b`，这时会去查询列`c_row`下的`c_inner_row`的`column_b`。**嵌套结构查询中，不能存在`table_name`**

### engine [string]

该 Transform 使用的 SQL 引擎。支持 `ZETA` 和 `INTERNAL`。如果不配置，默认使用 `ZETA`。

### where_rejected_update_as_delete [boolean]

是否把被 `WHERE` 子句拒绝的 `UPDATE_AFTER` 行作为其投影结果的 `DELETE` 发出。为 `false`（默认）时，它会像其他不满足条件的行一样被丢弃。仅当查询选出了更新不会改变的键、且 Sink 按该键删除行时才启用；参见 [CDC 流上的 WHERE](#cdc-流上的-where)。在 `table_transform` 中，请在每个需要的条目中单独设置：条目不会继承 Transform 级别的设置。

## 数值比较

当求值后的两个操作数均为整型（`TINYINT`、`SMALLINT`、`INT`、`BIGINT`）或 `DECIMAL` 时，比较保留数值精度。
此规则适用于 `=`、`!=`、`<>`、`<`、`<=`、`>`、`>=`、数值的 `IN` 和 `NOT IN` 判断，
以及搜索式和简单式 `CASE` 表达式中的比较。
例如，`BIGINT` 值 `9007199254740993` 不会匹配 `WHERE id = 9007199254740992`。

当任一操作数为 `FLOAT` 或 `DOUBLE` 时，仍使用浮点转换进行比较，保留 NaN、无穷大和带符号零的原有行为。
包含小数点的数值字面量求值为 `DOUBLE`，而非 `DECIMAL`：因此，将 `DECIMAL` 列与
`123456789012345678.98` 比较时，仍可能匹配 `123456789012345678.99`。
求值为整型的整数字面量（如 `-9007199254740993`）在另一操作数也是精确类型时使用精确比较。
对 `DECIMAL` 表达式应用一元负号会保留 `DECIMAL` 精度，因此在另一操作数也是精确类型时，
`-decimal_col` 和 `-CAST('...' AS DECIMAL(p, s))` 均保留精确的小数比较语义。

如需精确的小数常量，请使用带引号的字符串并保留足够的小数位，例如
`amount = CAST('123456789012345678.99' AS DECIMAL(38, 2))`。
对于负数常量，`CAST('-123456789012345678.99' AS DECIMAL(38, 2))` 和
`-CAST('123456789012345678.99' AS DECIMAL(38, 2))` 均保留小数精度。
现有 CAST 舍入行为不变：将字符串或 `DECIMAL` 转换为 `DECIMAL(p, s)` 时，
如果减少小数位，则采用 `CEILING`（向正无穷方向舍入）。因此，
`CAST('1.231' AS DECIMAL(10, 2))` 为 `1.24`，
`CAST('-1.239' AS DECIMAL(10, 2))` 为 `-1.23`。如果在 CAST 之后取负，
`-CAST('1.239' AS DECIMAL(10, 2))` 则为 `-1.24`：减少小数位时先舍入再取负，
因此这两种写法在该情况下不能互换。
转换 `FLOAT` 或 `DOUBLE` 时采用 `HALF_UP`；对带小数点的数值字面量进行 CAST，
无法恢复其求值为 `DOUBLE` 时已丢失的精度。

每一对操作数独立选择精确或浮点比较，包括 `IN` 列表中的每个元素。
当 `id = 9007199254740993` 时，`id IN (9007199254740992)` 为假，
但 `id IN (9007199254740992, 9007199254740992.0)` 为真，因为第二次比较使用 `DOUBLE`。
因此，在精确类型与浮点类型混用时，等号不一定满足传递性；需要精度保证时，请统一使用精确类型的操作数。
以上规则适用于 `ZETA` 和 `INTERNAL` 两种 SQL 引擎设置。

## CDC 流上的 WHERE

`WHERE` 子句逐行判断，并保留行类型。在 CDC 流中，一次更新由一条带旧值的 `UPDATE_BEFORE` 行和一条带新值的 `UPDATE_AFTER` 行组成。当更新使某行不再满足条件时，`UPDATE_BEFORE` 满足条件而被保留，`UPDATE_AFTER` 被丢弃。把 `UPDATE_AFTER` 写成 upsert 并跳过 `UPDATE_BEFORE` 的 Sink（例如带主键的 Jdbc）因此会保留旧行。

设置 `where_rejected_update_as_delete = true` 后，该 Transform 的输出如下：

| 行类型 | 是否满足 `WHERE` 子句 | 输出 |
|--------|-----------------------|------|
| 任意 | 是 | 投影后的行，行类型不变 |
| `UPDATE_AFTER` | 否 | 投影后的行，作为 `DELETE` |
| `INSERT`、`UPDATE_BEFORE`、`DELETE` | 否 | 无 |

这样，使某行不再满足条件的更新会以 `DELETE` 到达 Sink，按键写入的 Sink 会删除该行。使用默认值 `false` 时，所有不满足条件的行都会被丢弃，与之前的版本相同。

```hocon
transform {
  Sql {
    plugin_input = "cdc"
    plugin_output = "active_users"
    query = "select id, name, status from dual where status = 'active'"
    where_rejected_update_as_delete = true
  }
}
```

仅在以下两点同时成立时启用：

- **稳定的键：** 查询选出了 Sink 用来删除的键列，并且更新不会改变这些列。`DELETE` 只由新值构造，因此只能通过更新前后相同的键找到对应的行。键可以来自源表 Schema 的主键，也可以来自 Sink 的配置，例如 Jdbc 的 `primary_keys`。
- **会删除的 Sink：** Sink 按该键删除行。没有键的 Sink 会追加收到的每一行；例如，既没有 `primary_keys`、Schema 中也没有主键的 Jdbc Sink 会把 `DELETE` 写成一条新行。

投影的限制：

- 如果查询没有选出键，例如 Sink 以 `id` 为键时使用 `select name, status from dual where status = 'active'`，输出 Schema 中没有主键，`DELETE` 只包含被选出的列，Sink 无法找到对应的行。`primary_keys` 指定了查询未选出的列时，无论是否启用该选项，Jdbc Sink 都会在启动时失败。
- 旧值同样不满足条件时也会发送 `DELETE`。按键写入的 Sink 会删除一个它没有的键，数据不会改变，但 Sink 需要删除权限。Kudu Sink 在这种删除上会失败，除非设置 `ignore_not_found = true`。
- 使用 `LATERAL VIEW` 时，由被拒绝的 `UPDATE_AFTER` 产生的每一行都是 `DELETE`。
- 如果被拒绝的 `UPDATE_AFTER` 投影失败（例如 `select 10 / x as r from dual where x <> 0` 且 `x = 0`），该行会被丢弃，并记录一条警告日志。
- 启用该选项之前已经写入的目标表仍可能保留不再满足条件的行；请从源端重新加载以删除它们。

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

```hocon
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

## 集合表达式

ARRAY 和 MAP 的参数支持一元 `+`/`-` 和括号。例如，`ARRAY(-1, +2, (3))` 生成 `[-1, 2, 3]`，`MAP('negative', (-1))` 生成值为 `-1` 的映射。

带符号的集合表达式保留数值元素类型，并传递 NULL 操作数。TINYINT 数组现在使用 `ARRAY<TINYINT>`，替代此前错误的 `ARRAY<STRING>` 模式。

## 更新日志

- 支持 struct 类型查询

### 新版本

- 添加SQL转换连接器
