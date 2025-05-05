# StructEvolution 转换插件

> 在运行时动态演进表结构——新增、修改或删除列（并更新主键、索引、分区和注释）。

## 描述

StructEvolution 是一个基于 Transform‑V2 框架的插件，允许你在 SeaTunnel 的数据流中更改行的结构。你可以：

* **新增（ADD）** 列（指定位置、数据类型、可空性、默认值、注释）
* **修改（MODIFY）** 现有列（重命名、移动位置、更改类型、更新默认值或注释）
* **删除（DROP）** 不需要的列

## 配置项

| 名称            | 类型                   | 必需 | 默认值 |
|---------------|----------------------|----|-----|
| specific      | List<SpecificModify> | 是  | —   |
| plugin_input  | String               | 否  | —   |
| plugin_output | String               | 否  | —   |

### specific [config]

为某个逻辑表或视图定义一个或多个 schema 演进规则。每个 `SpecificModify` 块包含：

| 字段名         | 类型              | 必需 | 描述                          |
|-------------|-----------------|----|-----------------------------|
| input_name  | String          | 是  | 源表或视图名称                     |
| output_name | String          | 是  | 目标表或视图名称                    |
| columns     | List<Column>    | 否  | 列级操作（`ADD`、`MODIFY`、`DROP`） |
| primary_key | Primarykey      | 否  | 主键变更（重命名/重排主键列）             |
| indexes     | List<Index>     | 否  | 索引操作（新增或删除，支持指定列及排序方式）      |
| partition   | PartitionConfig | 否  | 分区配置变更                      |
| comment     | Comment         | 否  | 表注释的修改或删除                   |

#### 列操作示例

```hocon
columns = [
  {
    action        = "MODIFY"           # ADD、MODIFY 或 DROP
    input_name    = "old_col"          # 现有列
    output_name   = "new_col"          # 目标列名（MODIFY 或 ADD 时）
    position      = 2                  # 最终零基索引位置
    data_type     = "VARCHAR(50)"      # SQL 类型（ADD 或 MODIFY 时）
    nullable      = false              # 是否允许 NULL
    default_value = "N/A"              # 默认值（ADD 或 MODIFY 时）
    comment       = "Updated field"    # 列注释
  },
  { action = "DROP", input_name = "unused_col" },
  { action = "ADD",  input_name = "new_flag", output_name = "new_flag",
    position = 5, data_type = "BOOLEAN", nullable = true, default_value = false }
]
```

### 通用参数 [string]

Transform 插件通用参数，详情请参考 [Transform 通用选项](common-options.md)。

## 示例

假设源表 `employees` 如下：

| id | name        | salary | dept  |
|----|-------------|--------|-------|
| 1  | Alice Smith | 70000  | Sales |
| 2  | Bob Lee     | 80000  | Eng   |
| 3  | Carol Yang  | 75000  | HR    |

我们希望：

1. 将 `name` 重命名为 `full_name` 并移动到第 1 列（零基索引）
2. 删除 `dept` 列
3. 在末尾新增一个类型为 `DATE` 的 `join_date` 列（位置 3）

```hocon
transform {
  StructEvolution {
    plugin_input  = "employees"
    plugin_output = "employees_v2"
    specific = [
      {
        input_name  = "employees"
        output_name = "employees_v2"
        columns = [
          { action = "MODIFY", input_name = "name", output_name = "full_name", position = 1 },
          { action = "DROP",   input_name = "dept" },
          { action = "ADD",    input_name = "join_date", output_name = "join_date",
            position = 3, data_type = "DATE", nullable = true, default_value = null }
        ]
      }
    ]
  }
}
```

执行后，`employees_v2` 表将变为：

| id | full_name   | salary | join_date |
|----|-------------|--------|-----------|
| 1  | Alice Smith | 70000  | *null*    |
| 2  | Bob Lee     | 80000  | *null*    |
| 3  | Carol Yang  | 75000  | *null*    |

## 更新日志

### new version

