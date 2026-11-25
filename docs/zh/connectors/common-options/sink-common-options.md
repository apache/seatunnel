---
sidebar_position: 4
---

# Sink 常用选项

> Sink 连接器常用参数

关于 Sink 写入模式、自动生成 SQL、`schema_save_mode`、`data_save_mode`、`custom_sql`、
`enable_upsert` 的选择，请先阅读 [Sink 写入模式与 Save Mode](sink-write-modes.md)。

:::caution 警告

旧的配置名称 `source_table_name` 已经过时，请尽快迁移到新名称 `plugin_input`。

:::

| 名称           | 类型     | 是否需要 | 默认值 | 说明 |
|--------------|--------|------|-----|------|
| plugin_input | list   | 否    | -   | 不指定 `plugin_input` 时，插件按默认数据集标识 `DEFAULT_ID` 查找上游数据集，即绑定到未声明 `plugin_output` 的上游 source 或 transform 输出的数据集；若不存在这样的数据集，Zeta 引擎会回退绑定到作业中最后生成的数据集（兼容简单作业）。指定 `plugin_input` 时，当前插件处理该参数对应的数据集。该值可以是单个上游数据集名称，也可以是名称数组；配置多个名称时，Zeta 引擎会将这些上游数据集合并后写入当前 sink。 |
| metadata_datasource_id | string | 否    | -   | 用于从元数据中心获取连接配置的数据源 ID。指定后会从外部元数据服务获取连接信息（如 URL、用户名、密码），而不是使用直接配置。详见 [元数据 SPI](../../introduction/concepts/metadata-spi.md)。 |

### metadata_datasource_id [string]

用于从元数据中心获取连接配置的数据源 ID。当指定此参数时，连接器将从外部元数据服务获取连接详细信息（如 URL、用户名、密码），而不是使用直接配置。详见 [元数据 SPI](../../introduction/concepts/metadata-spi.md)。

### plugin_input [list]

当不指定 `plugin_input` 时，插件按默认数据集标识 `DEFAULT_ID` 查找上游数据集，而不是按配置文件中的位置相邻性绑定。未声明 `plugin_output` 的上游 source/transform 会以 `DEFAULT_ID` 发布自己的数据集，当前插件消费的就是该数据集。

当指定了 `plugin_input` 时，当前插件处理该参数对应的数据集。该值可以是单个上游数据集名称（字符串），也可以是名称数组；配置多个名称时，Zeta 引擎会将这些上游数据集合并后写入当前 sink。

## 重要说明

Sink 只消费数据集，因此没有 `plugin_output` 选项。`plugin_input` 的取值必须与上游 source 或 transform 声明的 `plugin_output` 一致。绑定关系按数据集标识解析，与插件在配置文件中的先后顺序无关：未指定 `plugin_input` 时，插件使用默认标识 `DEFAULT_ID`，所有未声明 `plugin_output` 的上游数据集都以该标识发布，因此多个 sink 可以在不写 `plugin_input` 的情况下消费同一个默认数据集。只有当作业不是简单结构（source、transform 或 sink 的数量多于一个）时，校验才要求每个 source/transform 声明 `plugin_output`、每个 transform/sink 声明 `plugin_input`（参见本页末尾的说明）。

## Examples

```bash
source {
    FakeSource {
      parallelism = 2
      plugin_output = "fake"
      schema = {
        fields {
          name = "string"
          age = "int"
        }
      }
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

> 如果作业只有一个 source、一个（或零个）transform 和一个 sink，可以不为连接器指定 `plugin_input` 和 `plugin_output`：上游输出以默认标识发布，未指定 `plugin_input` 的 transform/sink 按该默认标识绑定（没有匹配时回退到作业中最后生成的数据集）。
> 如果 source、transform 或 sink 中任意一类运算符的数量大于 1，作业会按命名数据集进行校验：每个 source/transform 必须声明 `plugin_output`，每个 transform/sink 必须声明 `plugin_input`。sink 只消费数据集，没有 `plugin_output`。
