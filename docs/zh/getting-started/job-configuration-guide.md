---
sidebar_position: 2
---

# 作业配置指南

SeaTunnel 的大多数作业都通过声明式配置完成。你通常不需要先写代码，而是通过配置文件描述执行环境、数据来源、可选转换以及写入目标。

本页的目标是帮助你理解一个 SeaTunnel 作业的基本结构、插件之间的数据流向，以及如何从内置示例平滑过渡到真实业务链路。

## 配置结构总览

大多数 SeaTunnel 作业都遵循相同的顶层结构：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 16
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

transform {
  FieldMapper {
    plugin_input = "fake"
    plugin_output = "renamed"
    field_mapper = {
      name = user_name
      age = age
    }
  }
}

sink {
  Console {
    plugin_input = "renamed"
  }
}
```

从职责上看：

- `env` 控制作业如何执行
- `source` 定义数据从哪里来
- `transform` 负责链路中的数据变换
- `sink` 定义数据最终写到哪里

## `env` 配置块

`env` 用于配置作业级别的执行参数。部分参数是所有引擎通用的，部分参数则和具体引擎相关。

常见参数包括：

| 参数 | 含义 |
| --- | --- |
| `job.mode` | `BATCH` 或 `STREAMING` |
| `parallelism` | 作业默认并行度 |
| `job.name` | 可选的作业显示名称 |
| `checkpoint.interval` | 流作业或 exactly-once 场景下的 checkpoint 间隔 |

如果使用 Flink 或 Spark，引擎特定参数也放在 `env` 中。详细规则请参考 [JobEnvConfig](../introduction/configuration/JobEnvConfig.md)。

## `source` 配置块

`source` 用于描述 SeaTunnel 如何从外部系统读取数据。

一个 source 通常包括：

- 连接器名称
- 连接参数
- 读取范围，例如表、topic、路径或查询
- schema 或 format 相关参数
- `plugin_output`，用于给下游插件显式引用当前输出

如果一个作业里有多个 source，建议显式命名每个 source 的输出，后续会更清晰。

## `transform` 配置块

`transform` 是可选的。当数据写入目标之前需要过滤、改名、映射、增强或校验时，可以在这里完成。

常见场景包括：

- 字段重命名或字段映射
- 行过滤
- RowKind 处理
- SQL 转换
- 写入前的数据校验

如果业务链路不需要中间转换，可以完全省略这一段，直接从 `source` 到 `sink`。

## `sink` 配置块

`sink` 用于定义 SeaTunnel 如何把数据写入目标系统。

一个 sink 通常包括：

- 连接器名称
- 连接参数
- 目标表、topic 或路径
- 写入语义或批处理相关参数
- `plugin_input`，用于声明当前 sink 消费哪个上游输出

不同 sink 的可选项不一样，具体参数、默认值和样例请以对应连接器文档为准。

## `plugin_input` 与 `plugin_output`

这两个字段是理解 SeaTunnel 数据流向的关键约定：

- `plugin_output` 用来给 source 或 transform 的输出命名
- `plugin_input` 用来让 transform 或 sink 指向某个上游输出

在以下场景中，它们尤其重要：

- 一个作业中存在多个 source
- 一个 transform 的输出要写入多个 sink
- 任务链路较复杂，需要保证配置可读性

如果链路只有一个上游，SeaTunnel 往往可以依赖默认约定继续向下流转；但从可维护性角度，仍然建议显式命名。

## 支持的配置格式

SeaTunnel 支持多种配置方式：

- **HOCON**：默认也是最常用的格式
- **JSON**：适合由其他系统自动生成配置
- **SQL**：适合 SQL 导向的作业表达方式

格式细节可继续阅读：

- [配置文件简介](../introduction/concepts/config.md)
- [SQL 配置](../introduction/configuration/sql-config.md)

## 如何从示例迁移到真实作业

从示例任务迁移到真实链路时，推荐按下面顺序逐步替换：

1. 保留示例中的 `env` 基本结构。
2. 用真实 Source 替换 `FakeSource`。
3. 用真实 Sink 替换 `Console`。
4. 只有在源表结构和目标结构不直接匹配时，再补充 transform。
5. 按连接器要求补充驱动 jar 或额外依赖。

例如：

- MySQL -> Doris
- Kafka -> Iceberg
- S3File -> StarRocks
- PostgreSQL CDC -> Kafka

## 运行前检查清单

在真正启动作业之前，建议确认以下事项：

- Java 和 `JAVA_HOME` 已正确配置
- 所需插件已经安装
- 第三方驱动 jar 已就位
- Source 凭据与网络访问正确
- 目标表、topic 或路径在需要时已提前创建
- `job.mode` 与连接器能力相匹配

## 下一步

- 需要先跑通一个可执行示例：查看 [SeaTunnel 引擎快速开始](./locally/quick-start-seatunnel-engine.md)
- 需要看一条更接近真实业务的完整链路：查看 [场景示例](./recipes/overview.md)
- 需要具体参数说明：查看 [数据来源连接器总览](../connectors/source-overview.md) 和 [数据写入连接器总览](../connectors/sink-overview.md)
- 需要了解转换能力：查看 [数据转换总览](../transforms)
- 需要理解引擎差异：查看 [执行引擎概览](../engines/overview.md)
