# SeaTunnel 运行在 Spark 上

当你的团队已经稳定运行 Spark，并希望 SeaTunnel 作业融入现有的批处理或混合负载环境时，Spark 会是合适的选择。如果你只是从零开始评估 SeaTunnel，且没有必须使用 Spark 的前提，建议先从 [SeaTunnel Engine](./zeta/about.md) 开始。

## 从这里开始

如果你的目标是让 SeaTunnel 跑在 Spark 上，建议按下面顺序阅读：

- [引擎概览](./overview.md)
- [Spark 引擎快速开始](../getting-started/locally/quick-start-spark.md)
- [作业配置指南](../getting-started/job-configuration-guide.md)

SeaTunnel 提供独立的 Spark 3 启动器。现有 Spark 3.3 部署可以继续使用原有运行时，
Spark 3.5 部署则可以使用 Spark 3.5.8 启动器：

- `seatunnel-spark-3-starter.jar` 用于 Spark 3.3.x
- `seatunnel-spark-3.5-starter.jar` 用于 Spark 3.5.x

现有 Spark 3.4 部署应继续使用原来的 `seatunnel-spark-3-starter.jar` 和
`start-seatunnel-spark-3-connector-v2` 启动脚本。本次变更不新增独立的 Spark 3.4
启动器，也不扩展其兼容性验证范围。

Spark 3.5 启动器复用了基于 Spark 3.3 编译的转换层。行编码器、批量写入和有界微批写入测试在
Spark 3.5.8 上验证了部分路径，但不能证明所有 Catalyst 或 DataSource V2 路径均兼容。
其他二进制兼容性问题可能仅在目标运行时执行相关路径时暴露。升级前请验证所用连接器
和部署模式。

Spark 3.5 公共启动器使用批处理的 `read()` 和 `write().save()` 执行路径，应使用批处理作业配置。
微批测试将有限的 Spark 文件流通过行编码器写入 SeaTunnel 流式 Sink 适配器，不覆盖 SeaTunnel
流式 Source、检查点恢复，或通过公共启动器提交流处理作业。Spark 3.5 连接器容器测试仅在定时
任务中运行（`testInPR=false`），普通 PR CI 不运行该运行时矩阵。

Spark 3.5 Windows `.cmd` 启动器仍将命令字符串传递给 `spark-submit.cmd`，未采用 Unix
启动器的参数文件协议。不支持包含嵌入引号或 Windows 命令解释器元字符的任意参数，
且尚未验证原生 Windows 启动器行为。请勿通过此启动器传入不可信的值；需要精确保留
参数时，请使用 Unix 启动器。

Windows 打包测试覆盖帮助输出、JVM 启动失败，以及打开包含空格、`!` 和 `&` 的带引号
配置路径。路径测试在配置解析错误处停止，不会提交 Spark 作业，因此不能证明
`spark-submit.cmd` 会精确保留参数。只有待验证版本的原生 Windows CI 通过后，
才能将这些启动器场景视为已验证。

Unix 打包测试运行真实的启动器 JVM，检查包含空格及单、双引号的路径参数是否完整保留。
该测试在 `spark-submit` 边界捕获参数，不会提交 Spark 作业。

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

使用 Spark 3.5 时，请使用 `start-seatunnel-spark-3.5-connector-v2.sh`，其他参数保持不变。

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

## 精度超过 38 的 DECIMAL

Spark 的 `DECIMAL` 类型精度最大为 38，而 SeaTunnel 的类型可以更宽，例如 Doris `LARGEINT`（`DECIMAL(39, 0)`）或 PostgreSQL `NUMERIC(50, 10)`。

- **顶层列和 `ROW` 内的字段**：SeaTunnel 在 Spark 中把值作为普通十进制文本放在 `STRING` 列里传递，并在列的 metadata 中记录声明的精度和小数位数。Transform 和 Sink 收到的仍然是 `DECIMAL(p, s)` 和原值，所以 Sink 自动建表得到的列类型与 Zeta、Flink 引擎一致。只有 Spark 自己看到的数据（例如 Dataset 的 schema 或临时视图）中，该列显示为 `STRING`。
- **在 `ARRAY` 或 `MAP` 内**（元素、键或值，任意嵌套层级）：没有可以存放 metadata 的列，因此作业在启动时、读取任何数据之前就会失败。报错信息包含表名、列路径（例如 `tags.element` 或 `attrs.value`）、声明的类型以及 Spark 的上限。

SeaTunnel 不会对这些值做四舍五入或截断。值超出声明的精度或小数位数时，作业会失败，并在报错中给出列名。

`ARRAY` 或 `MAP` 列的绕过办法。这些办法都在 Source 端生效，因为在 Spark 上 Source 的 schema 在任何 Transform 运行之前就已构建：

- 在 Source 端不读取该列，例如使用 Doris 的 `doris.read.field`，或在 JDBC 的 `query` 中不选该列；
- 在 Source 查询中把它转换为字符串；如果确定数值放得下，也可以转换为更窄的类型；
- 改用 Zeta 或 Flink 引擎运行作业，这两个引擎支持 `DECIMAL(p > 38)`。

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
