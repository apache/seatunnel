# SeaTunnel 运行在 Spark 上

当你的团队已经稳定运行 Spark，并希望 SeaTunnel 作业融入现有的批处理或混合负载环境时，Spark 会是合适的选择。如果你只是从零开始评估 SeaTunnel，且没有必须使用 Spark 的前提，建议先从 [SeaTunnel Engine](./zeta/about.md) 开始。

## 从这里开始

如果你的目标是让 SeaTunnel 跑在 Spark 上，建议按下面顺序阅读：

- [引擎概览](./overview.md)
- [Spark 引擎快速开始](../getting-started/locally/quick-start-spark.md)
- [作业配置指南](../getting-started/job-configuration-guide.md)

## 什么时候选择 Spark

以下场景通常更适合使用 Spark：

- 团队已经在生产中运行 Spark 集群
- 周边任务主要以批处理为主
- 希望 SeaTunnel 与既有 Spark 生态和部署方式保持一致

## Spark 专属配置如何写

Spark 专属作业参数写在 `env` 块中，并使用 `spark.` 前缀。

示例：

```hocon
env {
  spark.app.name = "example"
  spark.sql.catalogImplementation = "hive"
  spark.executor.memory = "2g"
  spark.executor.instances = "2"
  spark.yarn.priority = "100"
  spark.dynamicAllocation.enabled = "false"
}
```

## 命令行示例

Spark on YARN 集群模式：

```shell
./bin/start-seatunnel-spark-3-connector-v2.sh --master yarn --deploy-mode cluster --config config/example.conf
```

Spark on YARN 客户端模式：

```shell
./bin/start-seatunnel-spark-3-connector-v2.sh --master yarn --deploy-mode client --config config/example.conf
```

## 最小示例作业

下面这个例子会在 Spark 上运行，并把生成的数据打印到控制台。

```hocon
env {
  parallelism = 1

  spark.app.name = "example"
  spark.sql.catalogImplementation = "hive"
  spark.executor.memory = "2g"
  spark.executor.instances = "1"
  spark.yarn.priority = "100"
  spark.dynamicAllocation.enabled = "false"
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
    plugin_output = "fake1"
    field_mapper = {
      age = age
      name = new_name
    }
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
}
```

如果你需要更多 transform 能力，继续查看 [Transforms 目录](../transforms) 和 [Transform 通用参数](../transforms/common-options/common-options.md)。

## 从源码仓库运行示例

如果你是在源码仓库里运行示例，对应模块是：

- `seatunnel-examples/seatunnel-spark-connector-v2-example`

示例入口类是：

- `org.apache.seatunnel.example.spark.v2.SeaTunnelApiExample`

## 嵌套数组

对于 SeaTunnel 类型为数组的字段（包括连接器或目录提供的类型），Spark 转换层会在 Source、Transform 和 Sink 转换边界保留数组值，包括元素为数组、映射、行和小数的数组。例如，`ARRAY<ARRAY<INT>>` 字段会保留其内部数组，包括空数组。

配置中的 `schema` 字符串支持元素为字符串、布尔值、数值、映射和数组的数组类型，包括 `array<array<map<string,array<int>>>>`。行或小数的数组需要由连接器或目录提供类型；本次转换支持不会扩展配置模式解析器。

连接器的数据物化与 Spark 转换是不同的环节。对于配置解析器声明的多层映射数组，即使模式字符串能够成功解析，FakeSource 等基于 JSON 的 Source 目前仍无法物化数组的数组中的映射值。

null 元素（包括 null 映射和行）保持为 null。此前，数组中的 null 映射在 Spark Source 转换路径上会变为空映射（`{}`）。现在，返回给 SeaTunnel Transform 的数组使用声明的元素类型（例如 `String[]` 或 `Integer[]`），而不是通用的 `Object[]`，并且空数组在 Transform 和 Sink 边界保留其类型。迁移指南请参阅 [Spark 数组转换](../introduction/concepts/incompatible-changes.md#spark-数组转换)。

每个数组必须只有一种兼容的元素类型，不支持在同一数组中混合整数和数组等不同形状的元素。本次数组转换修复并不代表所有嵌套 SQL 表达式或多层映射流水线都已受支持。它不会新增从 Spark 数组模式自动推断 SeaTunnel 时间类型的能力，也不会改变模式配置或 SeaTunnel Sql 语法、扩展连接器自身的类型支持，或改变 Flink 转换层。

## 下一步

- [Spark 引擎快速开始](../getting-started/locally/quick-start-spark.md)
- [Spark 转换层](../architecture/api-design/spark-translation-layer.md)
- [Transforms 目录](../transforms)
- 如果你还想和默认引擎对比，可回看 [SeaTunnel Engine](./zeta/about.md)
