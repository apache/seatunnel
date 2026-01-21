# 行类型过滤

> 行类型转换插件

## 描述

按行类型过滤数据

## 操作

|      名称       |  类型   | 是否必须 | 默认值 |
|---------------|-------|------|-----|
| include_kinds | array | yes  |     |
| exclude_kinds | array | yes  |     |

### include_kinds [array]

要包含的行类型

### exclude_kinds [array]

要排除的行类型。

您只能配置 `include_kinds` 和 `exclude_kinds` 中的一个。

### common options [string]

转换插件的常见参数, 请参考  [Transform Plugin](common-options.md) 了解详情

## 示例

FakeSource 生成的数据的行类型是 `INSERT`。如果我们使用 `FilterRowKink` 转换并排除 `INSERT` 数据，我们将不会向接收器写入任何行。

```yaml

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
  FilterRowKind {
    plugin_input = "fake"
    plugin_output = "fake1"
    exclude_kinds = ["INSERT"]
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

