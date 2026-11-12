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

| 名称           | 类型     | 是否需要 | 默认值 |
|--------------|--------|------|-----|
| plugin_input | list   | 否    | -   |
| metadata_datasource_id | string | 否    | -   |
| multi_table_sink_replica | int | 否 | 1 |

### metadata_datasource_id [string]

用于从元数据中心获取连接配置的数据源 ID。当指定此参数时，连接器将从外部元数据服务获取连接详细信息（如 URL、用户名、密码），而不是使用直接配置。详见 [元数据 SPI](../../introduction/concepts/metadata-spi.md)。

### multi_table_sink_replica [int]

多表 Sink 在一个子任务中为每张表运行的 writer 数量，默认值为 1。取值大于 1 时，一张表的记录会被分配到
它的多个 writer 上，因此 writer 必须通过 `SupportMultiTableSinkWriter.replicaMode()` 声明如何安全地分配：

- `KEYED`：按 `primaryKey()` 给出的键路由，同一键的所有变更按顺序到达同一个 writer。声明该模式但没有键的
  writer 会被拒绝。Jdbc 声明该模式（`is_exactly_once` 除外），因此 Jdbc 表需要在表结构或 `primary_keys`
  中有主键。当表有 identifier 字段，或配置了 `iceberg.table.primary-keys` 或
  `iceberg.table.upsert-mode-enabled` 时，Iceberg 也声明该模式。
- `RANDOM`：记录可以分配到任意 writer，只适用于写入结果与写入顺序无关的 writer。Iceberg 只追加记录时声明该模式。
- `UNSUPPORTED`（默认）：创建或恢复 writer 时作业失败，错误信息会给出表、连接器和 writer 类。此类
  Sink 请将该选项设置为 1。

各连接器在 [#13223](https://github.com/apache/seatunnel/issues/13223) 中逐个验证并声明。

### plugin_input [list]

当不指定 `plugin_input` 时，当前插件处理配置文件中上一个插件输出的数据集 `dataset`。

当指定了 `plugin_input` 时，当前插件处理该参数对应的数据集。该值可以是单个上游数据集名称（字符串），也可以是名称数组；配置多个名称时，Zeta 引擎会将这些上游数据集合并后写入当前 sink。

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

> 注意：sink 只消费数据集，因此没有 `plugin_output` 选项。`plugin_input` 的值必须与上游 source 或 transform 声明的 `plugin_output` 一致。
>
> 如果作业只有一个 source 和一个（或零个）transform 和一个 sink ，则不需要为连接器指定 `plugin_input` 和 `plugin_output`。
> 如果 source 、transform 和 sink 中任意运算符的数量大于 1，则必须为作业中的每个连接器指定 `plugin_input` 和 `plugin_output`
