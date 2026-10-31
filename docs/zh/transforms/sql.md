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

### plugin_input [string]

源表名称，查询 SQL 表名称必须与此字段匹配。如果不配置，将使用输入目录表的表名。

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

## 模式演进（DDL）

当上游 source 发出模式变更事件时（例如 `MySQL-CDC` 配置了 `schema-changes.enabled = true`），SQL transform 会把每个列级变更翻译成自身输出的变更，而不是原样转发上游事件。因此到达 sink 的事件描述的始终是 sink 实际收到的列。只有 Zeta 引擎会把模式变更事件传递给 transform。

```hocon
transform {
  Sql {
    plugin_input = "products_cdc"
    plugin_output = "products_sql"
    query = "select id, name, weight, weight * 2 as double_weight from products"
  }
}
```

对于上面的查询，上游的 `ADD COLUMN description` 会被吸收，上游的 `MODIFY COLUMN name` 会以 `name` 列的 modify 到达 sink，上游的 `MODIFY COLUMN weight DECIMAL(12,3)` 会以 `weight` 列的 modify 到达 sink，并且由于 `weight * 2` 的推导类型从 DOUBLE 变为 DECIMAL(12,3)，还会以 `double_weight` 列的 modify 到达 sink。把 FLOAT 列 `weight` 改为 DOUBLE 的 `MODIFY COLUMN weight DOUBLE` 只会以 `weight` 列的 modify 到达 sink，因为 `weight * 2` 本来就被推导为 DOUBLE。

### 规则

| 上游变更 | 查询形态 | 对输出和 sink 的影响 |
|----------|----------|----------------------|
| `ADD COLUMN` | `select *` | 在相同位置新增该列。 |
| `ADD COLUMN` | `select *, expr AS x` | 在最后一个星号列之后、表达式列之前新增该列。 |
| `ADD COLUMN` | 不含 `*` 的查询 | 被吸收，不会到达 sink。 |
| `DROP COLUMN` | `select *` | 删除该列。 |
| `DROP COLUMN` | 查询未引用的列 | 被吸收。 |
| 删除或重命名查询引用的列 | 任意 | 作业以 `TRANSFORM_COMMON-09` 失败，除非同一条语句重新创建了同名列（见下文血缘说明）。 |
| 重命名（`CHANGE COLUMN`） | `select *` | 重命名该列。 |
| `MODIFY COLUMN` | `select *`、`select c`、`select c AS d` | 以新的源类型修改输出列。 |
| `MODIFY COLUMN` | 表达式列，例如 `weight * 2 AS double_weight` | 仅当表达式的推导类型发生变化时才修改该列；其 sink 类型由 SeaTunnel 类型转换得到，而不是复制源类型。 |
| `MODIFY COLUMN` | `cast(c AS ...)` | 被吸收，cast 固定了输出类型。 |
| 列注释 | `select *`、`select c` | 更新输出列的注释。 |
| 表注释 | 任意 | 原样转发。 |

血缘。变更按物理源列的身份归属到输出列。同一条语句中先重命名再新增同名列（`ALTER TABLE t CHANGE a b INT, ADD COLUMN a INT`）会在 sink 上重命名旧列并新增一列；同一条语句中先删除再重建同名列（`ALTER TABLE t DROP COLUMN a, ADD COLUMN a BIGINT`）会在 sink 上删除并重建该列，即使查询直接引用了 `a`。同一条语句中改名后又改回原名，则不产生任何变更。

故障恢复。从 checkpoint 或 savepoint 恢复后，source 会发出一个携带 checkpoint 时刻表结构的恢复事件。SQL transform 会基于该表结构重新计算查询，向下游传递自身恢复后的输出，并且不会由此推导任何 DDL，因此 checkpoint 之前已经执行过的 DDL 不会在 sink 上重复执行。恢复之后到达的 DDL 会基于恢复后的状态进行翻译。

### 限制

- 删除或重命名属于输出主键、约束键或分区键的列会使作业以 `TRANSFORM_COMMON-09` 失败，因为 sink 无法通过模式变更事件更新键；修改这类列是支持的。
- 同一条语句中形成环的重命名（`a -> b, b -> a`）会使作业失败。
- 会产生两个同名输出列的变更（例如 `select *, id AS age` 且源表新增了 `age`）会使作业失败。
- 类型变更会在事件到达时针对 select 列表以及 `WHERE` 中的比较（例如 `c > 0`）进行检查；仅出现在函数调用、UDF 或 lateral view 参数内部的类型变更不会在事件到达时被发现，会像以前一样在处理数据行时失败。
- 位于 SQL transform 之前的 transform 转发的事件必须能描述其自身输出。列顺序以上游产出的表为准：像 Metadata、RowKindExtractor 这类把自身追加的列放在最后的 transform 是可以接受的，新增列会被放在这些列之前。如果上游产出的表结构不包含事件所描述的列，作业会以 `TRANSFORM_COMMON-09` 失败，而不是写入错位的数据行。

## 集合表达式

ARRAY 和 MAP 的参数支持一元 `+`/`-` 和括号。例如，`ARRAY(-1, +2, (3))` 生成 `[-1, 2, 3]`，`MAP('negative', (-1))` 生成值为 `-1` 的映射。

带符号的集合表达式保留数值元素类型，并传递 NULL 操作数。TINYINT 数组现在使用 `ARRAY<TINYINT>`，替代此前错误的 `ARRAY<STRING>` 模式。

## 更新日志

- 支持模式演进（DDL）事件
- 支持 struct 类型查询

### 新版本

- 添加SQL转换连接器
