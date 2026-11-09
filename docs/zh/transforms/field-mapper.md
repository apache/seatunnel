# 字段映射

> 字段映射转换插件

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

FieldMapper 选择输入字段，并将其名称映射为输出字段名称。适用于字段重命名、调整字段顺序，以及删除下游 Sink 不需要的字段。只有 `field_mapper` 中指定的字段会保留在输出中，字段值和数据类型保持不变，表路径也不会改变。当数据源输出多张表时，可以配置共享映射，并通过完整的表级映射覆盖它。

## 属性

| 名称 | 类型 | 是否必须 | 默认值 | 描述 |
|------|------|----------|--------|------|
| field_mapper | Map&lt;String, String&gt; | 条件必填 | - | 从输入字段名到输出字段名的非空映射。未配置 `table_transform` 或其为空列表时，转换级别必须配置此项；每个 `table_transform` 条目也必须配置此项。 |
| table_match_regex | String | 否 | `.*` | 用于选择应用转换级别映射的表路径的正则表达式。 |
| table_transform | Array&lt;Map&gt; | 否 | `[]` | 通过精确匹配 `table_path` 为各表指定完整映射。 |
| rule_match_mode | String | 否 | `FIRST_MATCH` | 相同 `table_path` 的多个条目的应用方式。允许重复路径时必须显式配置。 |
| common options | - | 否 | - | 数据集连接参数，参见[转换插件通用配置](common-options/common-options.md)。 |

### field_mapper [Map&lt;String, String&gt;]

每个键是已有的输入字段名，对应的值是输出字段名。例如，`name = display_name` 将 `name` 重命名为 `display_name`，而 `id = id` 按原名称保留 `id`。未列为键的字段会被删除。此选项只改变字段选择和名称，不改变字段值或类型。

映射不能为空。每个键都必须存在于应用此规则的表的输入模式中；字段不存在会导致转换初始化失败。当 `table_transform` 包含条目时，可以省略转换级别的 `field_mapper`。如果提供了该选项，它仍然必须是有效的非空映射。

只有主键或约束键包含的所有列都保留在输出中时，才会保留该主键或约束键，并同步映射其列名。

输出列遵循映射顺序，而不是输入模式的字段顺序。例如，`field_mapper = { name = display_name, id = id }` 的输出列依次为 `display_name` 和 `id`，即使输入模式以 `id` 开头也是如此。

### table_match_regex [String]

与上游完整表路径匹配的正则表达式。默认值 `.*` 匹配所有表。例如，`demo[.](users|admins)` 匹配 `demo.users` 和 `demo.admins`。

此选项只用于选择应用转换级别 `field_mapper` 的表，不限制显式指定的 `table_transform` 条目。它匹配实际表路径，而不是 `plugin_input` 或 `plugin_output` 等数据集名称。没有转换级别映射时，此选项不会创建映射规则。

### table_transform [Array&lt;Map&gt;]

表级规则列表。每个条目必须包含：

- `table_path`：与上游表路径精确匹配的非空白字符串，例如 `demo.admins`。
- `field_mapper`：该表完整的非空映射。

表级条目优先于转换级别映射，并且不会继承其中的字段。即使转换级别映射保留了某个字段，如果精确匹配的表级映射省略了它，该字段仍然会被删除。此选项默认为空列表。

### rule_match_mode [String]

控制 `table_transform` 中具有相同 `table_path` 的多个条目的应用方式：

- `FIRST_MATCH`：只应用第一个匹配条目。
- `ALL_MATCH`：按列表顺序应用所有匹配条目。每次映射接收上一次映射产生的模式和行，因此其键必须使用仍然存在的字段的当前名称。

虽然默认值为 `FIRST_MATCH`，但省略此选项时，重复的 `table_path` 会被拒绝。如果需要为同一路径配置多个条目，必须显式将 `rule_match_mode` 设置为 `FIRST_MATCH` 或 `ALL_MATCH`。

### common options

通过 `plugin_input` 和 `plugin_output` 将转换连接到命名数据集。有关连接行为和已弃用的选项名称，参见[转换插件通用配置](common-options/common-options.md)。这些数据集名称不会重命名数据集中的表。

## 多表配置

对于每张上游表，FieldMapper 按以下顺序确定映射规则：

1. 如果存在精确匹配的 `table_transform` 条目，则应用选中的一条或多条表级规则。
2. 否则，如果存在转换级别的 `field_mapper`，且表路径匹配 `table_match_regex`，则应用该映射。
3. 否则，原样传递该表，其模式和行均保持不变。

只配置 `table_transform` 时，没有匹配条目的表保持不变。每个条目都会独立校验，即使同时存在转换级别映射也是如此。共享行为的详细说明参见[多表转换](transform-multi-table.md)。

## 示例

以下完整批处理配置使用 FakeSource 提供显式数据行，并通过 Console 输出结果，需要安装 FakeSource 和 Console 连接器。结果表按字段名标识值，Console 日志的实际显示格式可能不同。

### 仅使用转换级别映射

输入表 `demo.users`：

| id | name | age |
|----|------|-----|
| 1 | Alice | 20 |

保留 `id`，将 `name` 重命名为 `display_name`，并删除 `age`：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "input"
    schema {
      table = "demo.users"
      columns = [
        { name = "id", type = "int" },
        { name = "name", type = "string" },
        { name = "age", type = "int" }
      ]
    }
    rows = [{ kind = INSERT, fields = [1, "Alice", 20] }]
  }
}

transform {
  FieldMapper {
    plugin_input = "input"
    plugin_output = "mapped"
    field_mapper = {
      id = id
      name = display_name
    }
  }
}

sink {
  Console {
    plugin_input = "mapped"
  }
}
```

输出表 `demo.users`：

| id | display_name |
|----|--------------|
| 1 | Alice |

### 转换级别映射与表级覆盖规则

输入数据集包含三张表，每张表的模式均为 `id INT, name STRING, age INT`：

| 表路径 | id | name | age |
|--------|----|------|-----|
| demo.users | 1 | Alice | 20 |
| demo.admins | 2 | Bob | 30 |
| demo.audit | 3 | Carol | 40 |

正则表达式同时匹配 `demo.users` 和 `demo.admins`，但 `demo.admins` 的显式映射优先生效。`demo.audit` 不匹配任何规则，因此保持不变。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "input"
    tables_configs = [
      {
        schema {
          table = "demo.users"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" }
          ]
        }
        rows = [{ kind = INSERT, fields = [1, "Alice", 20] }]
      },
      {
        schema {
          table = "demo.admins"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" }
          ]
        }
        rows = [{ kind = INSERT, fields = [2, "Bob", 30] }]
      },
      {
        schema {
          table = "demo.audit"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" }
          ]
        }
        rows = [{ kind = INSERT, fields = [3, "Carol", 40] }]
      }
    ]
  }
}

transform {
  FieldMapper {
    plugin_input = "input"
    plugin_output = "mapped"
    table_match_regex = "demo[.](users|admins)"
    field_mapper = {
      id = id
      name = display_name
    }
    table_transform = [
      {
        table_path = "demo.admins"
        field_mapper = {
          id = admin_id
          name = admin_name
        }
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "mapped"
  }
}
```

预期输出，以字段名和值展示：

| 表路径 | 输出字段和值 |
|--------|--------------|
| demo.users | `id = 1`, `display_name = "Alice"` |
| demo.admins | `admin_id = 2`, `admin_name = "Bob"` |
| demo.audit | `id = 3`, `name = "Carol"`, `age = 40` |

### 仅使用表级映射

此配置使用与上一示例相同的输入表和数据行。由于省略了转换级别映射，只有 `demo.admins` 会被转换，`demo.users` 和 `demo.audit` 均保持不变。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "input"
    tables_configs = [
      {
        schema {
          table = "demo.users"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" }
          ]
        }
        rows = [{ kind = INSERT, fields = [1, "Alice", 20] }]
      },
      {
        schema {
          table = "demo.admins"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" }
          ]
        }
        rows = [{ kind = INSERT, fields = [2, "Bob", 30] }]
      },
      {
        schema {
          table = "demo.audit"
          columns = [
            { name = "id", type = "int" },
            { name = "name", type = "string" },
            { name = "age", type = "int" }
          ]
        }
        rows = [{ kind = INSERT, fields = [3, "Carol", 40] }]
      }
    ]
  }
}

transform {
  FieldMapper {
    plugin_input = "input"
    plugin_output = "mapped"
    table_transform = [
      {
        table_path = "demo.admins"
        field_mapper = {
          id = admin_id
          name = admin_name
        }
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "mapped"
  }
}
```

预期输出：

| 表路径 | 输出字段和值 |
|--------|--------------|
| demo.users | `id = 1`, `name = "Alice"`, `age = 20` |
| demo.admins | `admin_id = 2`, `admin_name = "Bob"` |
| demo.audit | `id = 3`, `name = "Carol"`, `age = 40` |

## 更新日志

### 新版本

- 添加 FieldMapper 转换插件
