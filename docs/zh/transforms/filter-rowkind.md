# 行类型过滤

## 支持的引擎

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark

## 描述

FilterRowKind 根据行的变更类型保留或丢弃数据。可以用它从上游变更流中选择插入、删除等事件。插件不会把一种行类型转换成另一种：保留的行仍具有原来的字段值、模式和行类型。被过滤的行不会继续传递到下游。

:::caution
丢弃更新或删除事件会改变 Sink 收到的变更流。请根据下游用途选择规则；行过滤不能替代对完整 CDC 变更流的处理。
:::

## 属性

| 名称 | 类型 | 是否必须 | 默认值 | 描述 |
|------|------|----------|--------|------|
| include_kinds | Array&lt;Enum&gt; | 条件必需 | - | 只保留列表中的行类型。与 `exclude_kinds` 互斥，列表不能为空。 |
| exclude_kinds | Array&lt;Enum&gt; | 条件必需 | - | 丢弃列表中的行类型。与 `include_kinds` 互斥，列表不能为空。 |
| table_match_regex | String | 否 | `.*` | 匹配要应用转换级规则的上游表路径。 |
| table_transform | Array&lt;Map&gt; | 否 | `[]` | 通过精确的 `table_path` 为各表指定完整规则。 |
| rule_match_mode | Enum | 否 | `FIRST_MATCH` | 选择第一条匹配的表级规则，或按顺序执行所有匹配的规则。重复路径的行为见下文。 |
| common options | - | 否 | - | `plugin_input`、`plugin_output` 等转换插件通用参数。 |

### include_kinds [Array&lt;Enum&gt;]

仅保留行类型在列表中的数据，例如 `include_kinds = ["INSERT", "UPDATE_AFTER"]`。支持以下值：

| 值 | 含义 |
|----|------|
| `INSERT` | 插入的行。 |
| `UPDATE_BEFORE` | 更新前的行内容。 |
| `UPDATE_AFTER` | 更新后的行内容。 |
| `DELETE` | 删除的行。 |

没有非空 `table_transform` 列表时，转换级配置必须且只能指定 `include_kinds` 和 `exclude_kinds` 中的一个。两者都配置、两者都不配置或所选列表为空，都会导致校验失败。`table_transform = []` 不能免除这个要求。

配置了表级条目时，可以不设置转换级的包含/排除规则；如果设置了，仍必须且只能包含一个非空列表。每个表级条目也必须独立满足相同要求。

### exclude_kinds [Array&lt;Enum&gt;]

丢弃行类型在列表中的数据，保留其他行。支持的四个值与 `include_kinds` 相同，没有默认值。例如 `exclude_kinds = ["DELETE", "UPDATE_BEFORE"]` 会保留插入行和更新后的行。

同一条规则不能同时配置这两个选项。表级规则会替代该表的转换级规则，因此可以选择与转换级规则不同的选项。

### table_match_regex [String]

针对完整上游表路径匹配的 Java 正则表达式，例如 `test.abc` 或 `database.schema.table`。默认值 `.*` 匹配所有表路径。这里匹配的是实际表路径，不是 `plugin_input` 或 `plugin_output` 中的数据集名称。

此选项为转换级规则选择表，不限制显式的 `table_transform` 条目。未配置转换级包含/排除选项时，仅设置此正则表达式不会产生过滤规则。

### table_transform [Array&lt;Map&gt;]

表级规则列表，默认为空列表。每个条目必须包含非空白的 `table_path`，并且必须且只能包含一个非空的 `include_kinds` 或 `exclude_kinds` 列表。`table_path` 是精确路径，不是正则表达式。

每个条目都是完整规则，不继承转换级的包含/排除列表。匹配的条目具有更高优先级，即使该表不匹配 `table_match_regex` 也会使用该条目。

### rule_match_mode [Enum]

控制多个条目具有相同精确 `table_path` 时的行为：

- 显式设置 `FIRST_MATCH`：按列表顺序使用第一条匹配的条目。
- 显式设置 `ALL_MATCH`：按列表顺序执行所有匹配的条目。只有通过每个过滤器的行才会保留；这不是对各条目保留类型取并集。

虽然选项的默认值为 `FIRST_MATCH`，但省略此选项时，重复的 `table_path` 会在转换初始化阶段被拒绝。如果确实需要重复路径，请显式设置此选项。路径不重复时，使用默认值即可。

### common options

使用 `plugin_input` 和 `plugin_output` 连接上游和下游插件。通用参数详见[转换插件](common-options/common-options.md)。

## 多表配置

对于每个输入表，按照以下顺序选择有效规则：

1. 根据 `rule_match_mode` 使用匹配的一个或多个 `table_transform` 条目。
2. 否则，如果配置了转换级规则且表路径匹配 `table_match_regex`，使用转换级规则。
3. 否则，原样传递该表的行。

仅配置 `table_transform` 时，未列出的表保持不变。FilterRowKind 过滤的是行，而不是目录表定义；即使某个表的所有行都被丢弃，其模式仍可保留在输出中。

下面的示例涵盖三种配置方式。共享匹配规则详见[多表转换](transform-multi-table.md)。

## 示例

以下完整批处理配置使用 FakeSource 和 Console。在 SeaTunnel 发行包中安装这两个连接器后，可以将配置保存为文件，并通过 `bin/seatunnel.sh --config <file> -e local` 运行。这些示例生成的行类型为 `INSERT`，随机字段值不影响预期行数。

### 仅使用转换级规则

输入：`test.abc` 的三条 `INSERT` 行。输出：零行，因为规则排除了 `INSERT`。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  FakeSource {
    plugin_output = "fake"
    parallelism = 1
    row.num = 3
    schema {
      table = "test.abc"
      fields { id = "int" }
    }
  }
}

transform {
  FilterRowKind {
    plugin_input = "fake"
    plugin_output = "filtered"
    exclude_kinds = ["INSERT"]
  }
}

sink {
  Console {
    plugin_input = "filtered"
  }
}
```

### 转换级规则与表级规则组合

每个输入表都有三条 `INSERT` 行。`test.xyz` 的显式条目覆盖转换级排除规则。`test.www` 不匹配正则表达式，因此保持不变。

| 表 | 输入行数 | 输出行数 | 有效规则 |
|----|----------|----------|----------|
| `test.abc` | 3 | 0 | 转换级 `exclude_kinds = ["INSERT"]`。 |
| `test.xyz` | 3 | 3 | 表级 `include_kinds = ["INSERT"]`。 |
| `test.www` | 3 | 3 | 没有匹配的规则，保持不变。 |

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  FakeSource {
    plugin_output = "fake"
    parallelism = 1
    tables_configs = [
      {
        row.num = 3
        schema {
          table = "test.abc"
          columns = [{ name = "id", type = "int" }]
        }
      },
      {
        row.num = 3
        schema {
          table = "test.xyz"
          columns = [{ name = "id", type = "int" }]
        }
      },
      {
        row.num = 3
        schema {
          table = "test.www"
          columns = [{ name = "id", type = "int" }]
        }
      }
    ]
  }
}

transform {
  FilterRowKind {
    plugin_input = "fake"
    plugin_output = "filtered"
    table_match_regex = "test[.](abc|xyz)"
    exclude_kinds = ["INSERT"]
    table_transform = [
      {
        table_path = "test.xyz"
        include_kinds = ["INSERT"]
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "filtered"
  }
}
```

### 仅使用表级规则

未配置转换级包含/排除规则。只有 `test.xyz` 的插入行被丢弃，其他表保持不变。

| 表 | 输入行数 | 输出行数 |
|----|----------|----------|
| `test.abc` | 3 | 3 |
| `test.xyz` | 3 | 0 |
| `test.www` | 3 | 3 |

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  FakeSource {
    plugin_output = "fake"
    parallelism = 1
    tables_configs = [
      {
        row.num = 3
        schema {
          table = "test.abc"
          columns = [{ name = "id", type = "int" }]
        }
      },
      {
        row.num = 3
        schema {
          table = "test.xyz"
          columns = [{ name = "id", type = "int" }]
        }
      },
      {
        row.num = 3
        schema {
          table = "test.www"
          columns = [{ name = "id", type = "int" }]
        }
      }
    ]
  }
}

transform {
  FilterRowKind {
    plugin_input = "fake"
    plugin_output = "filtered"
    table_transform = [
      {
        table_path = "test.xyz"
        exclude_kinds = ["INSERT"]
      }
    ]
  }
}

sink {
  Console {
    plugin_input = "filtered"
  }
}
```
