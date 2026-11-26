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
| plugin_input | list | Zeta 复杂作业必填 | - | 上游数据集名称或名称列表。 |
| metadata_datasource_id | string | 否    | -   | 用于从元数据中心获取连接配置的数据源 ID。指定后会从外部元数据服务获取连接信息（如 URL、用户名、密码），而不是使用直接配置。详见 [元数据 SPI](../../introduction/concepts/metadata-spi.md)。 |

### metadata_datasource_id [string]

用于从元数据中心获取连接配置的数据源 ID。当指定此参数时，连接器将从外部元数据服务获取连接详细信息（如 URL、用户名、密码），而不是使用直接配置。详见 [元数据 SPI](../../introduction/concepts/metadata-spi.md)。

### plugin_input [list]

将 `plugin_input` 设为上游 source 或 transform 通过 `plugin_output` 声明的数据集名称。可填写单个名称（字符串）或名称数组。选择多个数据集时，Zeta 会合并兼容的输入。

## 重要说明

对于只有一个 source、一个 sink 且最多一个 transform 的 Zeta 作业，省略 `plugin_output` 或 `plugin_input` 时使用默认数据集标识 `DEFAULT_ID`。若 sink 的输入名称均未匹配到数据集，Zeta 会回退到最后生成的数据集。

如果 Zeta 作业包含多个 source、多个 transform 或多个 sink，每个 source 和 transform 都必须设置 `plugin_output`，每个 transform 和 sink 都必须设置 `plugin_input`。多个 sink 可以将各自的 `plugin_input` 设为同一个输出名称，消费同一数据集。

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
