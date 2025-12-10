# Pivot

> Pivot（行列转换）转换插件

## 描述

Pivot 转换插件可以将多行数据转换为列，通过对指定列进行透视操作。这对于将规范化数据转换为非规范化格式非常有用，其中一列的值会变成新的列名。

该转换支持**带检查点功能的批量处理**，这意味着它可以累积数据，并通过 SeaTunnel 的检查点机制在故障后恢复。

## 配置选项

|       名称         |  类型  | 是否必填 |  默认值  |
|-------------------|--------|----------|---------|
| group_by_keys     | array  | 是       |         |
| pivot_column      | string | 是       |         |
| value_column      | string | 是       |         |
| pivot_values      | array  | 是       |         |
| default_value     | string | 否       | null    |
| max_buffer_size   | int    | 否       | 10000   |
| group_timeout_ms  | long   | 否       | -1      |

### group_by_keys [array]

用于对行进行分组的列。具有相同值的行将被合并为单个输出行。

### pivot_column [string]

其值将成为输出中新列名的列。该列中的每个唯一值都会创建一个新列。

### value_column [string]

其值将填充到新透视列中的列。

### pivot_values [array]

预定义的透视值列表。只有 `pivot_column` 中的这些值才会创建新列。这是定义输出模式所必需的。

### default_value [string]

当某个分组缺少透视值时使用的默认值。默认为 null。

### max_buffer_size [int]

强制刷新前要缓冲的最大分组数。这有助于控制流处理场景中的内存使用。设置为 -1 表示无限缓冲（仅在检查点时刷新）。默认值为 10000。

### group_timeout_ms [long]

分组的超时时间（毫秒）。如果分组在此超时时间内没有收到新数据，则会被刷新。设置为 -1 禁用基于超时的刷新。默认值为 -1。

### common options [string]

转换插件通用参数，请参阅 [Transform Plugin](common-options.md) 了解详情。

## 工作原理

Pivot 转换的工作流程：

1. **收集**传入的行，并按 `group_by_keys` 分组
2. **提取** `pivot_column` 中的透视键和 `value_column` 中的值
3. **存储**每个分组对应透视列中的值
4. **刷新**在检查点或缓冲区满时输出累积的分组
5. **输出**每个分组一行，包含原始分组列和新的透视列

## 示例

### 基本示例

从源读取的数据如下表所示：

| id | type | value |
|----|------|-------|
| 1  | A    | 100   |
| 1  | B    | 200   |
| 2  | A    | 150   |
| 2  | C    | 300   |

我们想要透视 `type` 列，使用 `value` 列的值，按 `id` 分组：

```hocon
transform {
  Pivot {
    plugin_input = "fake"
    plugin_output = "pivoted"
    group_by_keys = ["id"]
    pivot_column = "type"
    value_column = "value"
    pivot_values = ["A", "B", "C"]
  }
}
```

结果表 `pivoted` 中的数据将是：

| id | A   | B    | C    |
|----|-----|------|------|
| 1  | 100 | 200  | null |
| 2  | 150 | null | 300  |

### 多分组键示例

对于具有多个分组列的数据：

| store_id | date       | metric  | value |
|----------|------------|---------|-------|
| 1        | 2024-01-01 | sales   | 1000  |
| 1        | 2024-01-01 | returns | 50    |
| 1        | 2024-01-02 | sales   | 1200  |
| 2        | 2024-01-01 | sales   | 800   |

配置：

```hocon
transform {
  Pivot {
    plugin_input = "source"
    plugin_output = "pivoted"
    group_by_keys = ["store_id", "date"]
    pivot_column = "metric"
    value_column = "value"
    pivot_values = ["sales", "returns", "profit"]
  }
}
```

结果：

| store_id | date       | sales | returns | profit |
|----------|------------|-------|---------|--------|
| 1        | 2024-01-01 | 1000  | 50      | null   |
| 1        | 2024-01-02 | 1200  | null    | null   |
| 2        | 2024-01-01 | 800   | null    | null   |

### 完整作业配置

```hocon
env {
  job.mode = "BATCH"
  checkpoint.interval = 10000
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        id = "int"
        type = "string"
        value = "int"
      }
    }
    rows = [
      { fields = [1, "A", 100], kind = INSERT }
      { fields = [1, "B", 200], kind = INSERT }
      { fields = [2, "A", 150], kind = INSERT }
      { fields = [2, "C", 300], kind = INSERT }
    ]
  }
}

transform {
  Pivot {
    plugin_input = "fake"
    plugin_output = "pivoted"
    group_by_keys = ["id"]
    pivot_column = "type"
    value_column = "value"
    pivot_values = ["A", "B", "C"]
  }
}

sink {
  Console {
    plugin_input = "pivoted"
  }
}
```

## 检查点支持

Pivot 转换实现了带检查点支持的有状态处理：

- **状态持久化**：缓冲的分组在检查点期间被保存
- **容错性**：故障恢复后，转换从最后一个检查点恢复其缓冲区
- **至少一次语义**：数据在每个检查点时刷新，确保不会丢失数据

这对于转换随时间累积数据的流处理作业特别重要。

## 更新日志

### 新版本

- 添加支持检查点的 Pivot 转换连接器
