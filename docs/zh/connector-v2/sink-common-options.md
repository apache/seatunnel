---
sidebar_position: 4
---

# Sink 常用选项

> Sink 连接器常用参数

:::caution 警告

旧的配置名称 `source_table_name` 已经过时，请尽快迁移到新名称 `plugin_input`。

:::

| 名称           | 类型     | 是否需要 | 默认值 |
|--------------|--------|------|-----|
| plugin_input | string | 否    | -   |
| parallelism  | int    | 否    | -   |

### plugin_input [string]

当不指定 `plugin_input` 时，当前插件处理配置文件中上一个插件输出的数据集 `dataset`

当指定了 `plugin_input` 时，当前插件正在处理该参数对应的数据集

### parallelism [int]

当没有指定`parallelism`时，默认使用 env 中的 `parallelism`。

当指定 `parallelism` 时，它将覆盖 env 中的 `parallelism`。

## Examples

```bash
source {
    FakeSourceStream {
      parallelism = 2
      plugin_output = "fake"
      field_name = "name,age"
    }
}

transform {
    Filter {
      plugin_input = "fake"
      fields = [name]
      plugin_output = "fake_name"
    }
    Filter {
      plugin_input = "fake"
      fields = [age]
      plugin_output = "fake_age"
    }
}

sink {
    Console {
      plugin_input = "fake_name"
    }
    Console {
      plugin_input = "fake_age"
    }
}
```

> 如果作业只有一个 source 和一个（或零个）transform 和一个 sink ，则不需要为连接器指定 `plugin_input` 和 `plugin_output`。
> 如果 source 、transform 和 sink 中任意运算符的数量大于 1，则必须为作业中的每个连接器指定 `plugin_input` 和 `plugin_output`

