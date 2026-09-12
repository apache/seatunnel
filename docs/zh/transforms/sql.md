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

对于上面的查询，上游的 `ADD COLUMN description` 会被吸收，上游的 `MODIFY COLUMN name` 会以 `name` 列的 modify 到达 sink，上游的 `MODIFY COLUMN weight` 会以 `weight` 和 `double_weight` 两列的 modify 到达 sink。

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

### 限制

- 删除或重命名属于输出主键、约束键或分区键的列会使作业以 `TRANSFORM_COMMON-09` 失败，因为 sink 无法通过模式变更事件更新键；修改这类列是支持的。
- 同一条语句中形成环的重命名（`a -> b, b -> a`）会使作业失败。
- 会产生两个同名输出列的变更（例如 `select *, id AS age` 且源表新增了 `age`）会使作业失败。
- 类型变更会在事件到达时针对 select 列表以及 `WHERE` 中的比较（例如 `c > 0`）进行检查；仅出现在函数调用、UDF 或 lateral view 参数内部的类型变更不会在事件到达时被发现，会像以前一样在处理数据行时失败。
- 位于 SQL transform 之前的 transform 转发的事件必须能描述其自身输出。如果上游产出的表结构与事件不一致，作业会以 `TRANSFORM_COMMON-09` 失败，而不是写入错位的数据行。

## 更新日志

- 支持模式演进（DDL）事件

### 新版本

- 添加SQL转换连接器
