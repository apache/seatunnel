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

Spark 4.1 on YARN 客户端模式（JDK 17+，使用 `-spark41-bin` 发行包）：

```shell
./bin/start-seatunnel-spark-4.1-connector-v2.sh --master yarn --deploy-mode client --config config/example.conf
```

## Spark 4.1 发行包

Spark 4.1 需要 **JDK 17 或更高版本**，并使用单独的 binary 发行包：

- `apache-seatunnel-${version}-bin.tar.gz` — Spark 2.4 / 3.3、Flink 与 SeaTunnel Engine（JDK 8+）
- `apache-seatunnel-${version}-spark41-bin.tar.gz` — Spark 4.1 starter 与精简 connector 集合（JDK 17+）

Spark 4.1 发行包当前包含 starter 日志依赖、`connector-fake`、`connector-console`、`connector-assert`、Scala 2.13 运行时库（`scala-library`、`scala-reflect`）以及常用 JDBC/Hadoop 可选库。如需更多 connector，请使用 `bin/install-plugin.sh` 安装。

### Spark 4.1 支持范围

当前 Spark 4.1 首发版本聚焦 **最小 source → sink 路径**。**不会**在 `-spark41-bin` 发行包中附带 `seatunnel-transforms-v2`，因为 transform 插件仍基于 Scala 2.12 编译，与 Spark 4.1 的 Scala 2.13 运行时存在冲突。

- 当前已支持：Spark 4.1 上的 source / sink connector（例如 `FakeSource` → `Console` / `Assert`）
- 当前 `-spark41-bin` 发行包尚未支持：含 `FieldMapper`、`Sql` 等 `seatunnel-transforms-v2` 插件的 transform 阶段
- 后续工作见 [design issue #11184](https://github.com/apache/seatunnel/issues/11184)

如果作业包含 `transform { ... }` 块，请使用标准 `-bin` 包配合 Spark 2.4 / 3.3，或使用 SeaTunnel Engine，待 Spark 4.1 transform 支持完成后再切换。

## 最小示例作业（Spark 2.4 / 3.3）

下面这个例子适用于 Spark 2.4 / 3.3，会在 Spark 上运行并把生成的数据打印到控制台，包含 `FieldMapper` transform。

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

## Spark 4.1 最小示例作业

请配合 `-spark41-bin` 发行包使用以下不含 transform 的作业配置，与当前 Spark 4.1 E2E 冒烟测试（`FakeSource` → `Assert`）一致：

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"

  spark.app.name = "spark41-example"
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

sink {
  Assert {
    plugin_input = "fake"
    rules = {
      field_rules = [
        {
          field_name = name
          field_type = string
          field_value = [
            {
              rule_type = NOT_NULL
            }
          ]
        }
      ]
    }
  }
}
```

## 从源码仓库运行示例

如果你是在源码仓库里运行示例，对应模块是：

- `seatunnel-examples/seatunnel-spark-connector-v2-example`

示例入口类是：

- `org.apache.seatunnel.example.spark.v2.SeaTunnelApiExample`

## 下一步

- [Spark 引擎快速开始](../getting-started/locally/quick-start-spark.md)
- [Spark 转换层](../architecture/api-design/spark-translation-layer.md)
- [Transforms 目录](../transforms)
- 如果你还想和默认引擎对比，可回看 [SeaTunnel Engine](./zeta/about.md)
