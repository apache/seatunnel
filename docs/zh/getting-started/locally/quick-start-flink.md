---
sidebar_position: 3
---

# Flink 引擎快速开始

## 开始前建议先看

这一页适合已经明确要把 SeaTunnel 跑在 Flink 上的团队。如果你只是第一次评估 SeaTunnel，且没有必须使用 Flink 的前提，建议先从 [SeaTunnel 引擎快速开始](./quick-start-seatunnel-engine.md) 开始。

如有需要，建议先看这几页：

- [引擎概览](../../engines/overview.md)
- [SeaTunnel 运行在 Flink 上](../../engines/flink.md)
- [作业配置指南](../job-configuration-guide.md)

## 步骤 1: 部署SeaTunnel及连接器

在开始前，请确保您已经按照[部署](deployment.md)中的描述下载并部署了SeaTunnel。

## 步骤 2: 部署并配置Flink

请先[下载Flink](https://flink.apache.org/downloads.html)(**需要版本 >= 1.12.0**)。更多信息您可以查看[入门: Standalone模式](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/standalone/overview/)

**配置SeaTunnel**: 修改`config/seatunnel-env.sh`中的设置，将`FLINK_HOME`配置设置为Flink的部署目录。

## 步骤 3: 添加作业配置文件来定义作业

编辑`config/v2.streaming.conf.template`，它决定了SeaTunnel启动后数据输入、处理和输出的方式及逻辑。
下面是配置文件的示例，它与上面提到的示例应用程序相同。

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

关于配置的更多信息请查看[配置的基本概念](../../introduction/concepts/config.md)

如果你还需要了解数据集编排或 Transform 参数，可继续阅读：

- [作业配置指南](../job-configuration-guide.md)
- [Transform 通用参数](../../transforms/common-options/common-options.md)

## 步骤 4: 运行SeaTunnel应用程序

您可以通过以下命令启动应用程序：

Flink版本`1.12.x`到`1.14.x`

```shell
cd "apache-seatunnel-${version}"
./bin/start-seatunnel-flink-13-connector-v2.sh --config ./config/v2.streaming.conf.template
```

Flink版本`1.15.x`到`1.18.x`

```shell
cd "apache-seatunnel-${version}"
./bin/start-seatunnel-flink-15-connector-v2.sh --config ./config/v2.streaming.conf.template
```

Flink版本`1.20.x`

```shell
cd "apache-seatunnel-${version}"
./bin/start-seatunnel-flink-20-connector-v2.sh --config ./config/v2.streaming.conf.template
```

**查看输出**: 当您运行该命令时，您可以在控制台中看到它的输出。您可以认为这是命令运行成功或失败的标志。

SeaTunnel控制台将会打印一些如下日志信息:

日志前缀（时间、线程、日志格式）以及 `tableId` 的取值取决于引擎的日志配置和具体作业，下面以 `...` 表示。`FieldMapper` 按 `field_mapper` 中的顺序输出字段，并把 `name` 重命名为 `new_name`，因此输出的 row type 为 `age<INT>, new_name<STRING>`。

```shell
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - output rowType: age<INT>, new_name<STRING>
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=1:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 1984352560, elWaB
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=2:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 762961563, uAtnp
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=3:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 2042675010, TQEIB
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=4:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 593971283, DcFjo
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=5:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 2099913608, SenEb
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=6:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 1928005856, DHjkg
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=7:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 526029657, eScCM
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=8:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 600878991, sgOeE
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=9:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 1951126920, gwdvw
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=10:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 488708928, nSiKE
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=11:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 1420202810, xubpl
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=12:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 331185742, rHZqb
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=13:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 1112878259, rciGD
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=14:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 1457046294, qLhdI
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=15:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 1240668386, ZTkRx
... INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0  rowIndex=16:  SeaTunnelRow#tableId=... SeaTunnelRow#kind=INSERT : 94186144, SGZCr
```

## 此外

- 开始编写您自己的配置文件，选择您想要使用的[连接器](../../connectors/source)，并根据连接器的文档配置参数。
- 如果您想要了解更多关于SeaTunnel运行在Flink上的信息，请参阅[基于Flink的SeaTunnel](../../engines/flink.md)。
- 如果您想理解 SeaTunnel API 如何被适配到 Flink，请继续看 [Flink 转换层](../../architecture/api-design/flink-translation-layer.md)。
- SeaTunnel 内置了 `Zeta` 引擎，它也是默认推荐引擎。如果您希望走最短的本地验证路径，可以回到 [SeaTunnel 引擎快速开始](quick-start-seatunnel-engine.md)。
