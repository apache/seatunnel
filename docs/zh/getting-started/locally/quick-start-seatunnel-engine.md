---
sidebar_position: 2
---

# SeaTunnel 引擎快速开始

SeaTunnel Engine 既可以用于单机快速体验，也可以部署为多节点集群。本页按照这两种使用方式组织：

| 使用方式 | 适用场景 | 下一步 |
| --- | --- | --- |
| 单机快速开始 | 在一台机器上验证配置、连接器或处理链路 | 继续阅读本页的单机快速开始部分 |
| 集群部署 | 在测试、预发或生产环境中运行多节点任务 | 跳转到 [SeaTunnel Engine(Zeta) 安装部署](../../engines/zeta/deployment.md) |

## 第一部分：单机快速开始（Local 模式）

这一部分适合在单台机器上快速验证安装、连接器和作业配置。下面的命令都使用 `-m local` 启动 SeaTunnel Engine。

## 开始前建议先看

如果这是你第一次进入 SeaTunnel 文档，建议按下面顺序阅读：

- [快速入门总览](../overview.md)
- [安装部署](deployment.md)
- [作业配置指南](../job-configuration-guide.md)

本页示例链路使用 `FakeSource`、`FieldMapper` 和 `Console`。在运行示例前，请先确认相关插件已经安装：

```plugin_config
--seatunnel-connectors--
connector-fake
connector-console
--end--
```

```bash
sh bin/install-plugin.sh
```

### 步骤 1: 部署SeaTunnel及连接器

在开始前，请确保您已经按照[部署](deployment.md)中的描述下载并部署了SeaTunnel。

如果你已经安装了完整插件，可以直接复用。若只是为了最短路径跑通本页示例，上面列出的两个插件就足够。

### 步骤 2: 添加作业配置文件来定义作业

编辑`config/v2.batch.config.template`，它决定了当seatunnel启动后数据输入、处理和输出的方式及逻辑。
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

### 步骤 3: 运行SeaTunnel应用程序

您可以通过以下命令启动应用程序：

:::tip

从2.3.1版本开始，seatunnel.sh中的-e参数被废弃，请改用-m参数。

:::

```shell
cd "apache-seatunnel-${version}"
./bin/seatunnel.sh --config ./config/v2.batch.config.template -m local

```

**查看输出**: 当您运行该命令时，您可以在控制台中看到它的输出。您可以认为这是命令运行成功或失败的标志。

SeaTunnel控制台将会打印一些如下日志信息:

```shell
2022-12-19 11:01:45,417 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - output rowType: name<STRING>, age<INT>
2022-12-19 11:01:46,489 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=1:  SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: CpiOd, 8520946
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=2: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: eQqTs, 1256802974
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=3: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: UsRgO, 2053193072
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=4: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: jDQJj, 1993016602
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=5: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: rqdKp, 1392682764
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=6: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: wCoWN, 986999925
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=7: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: qomTU, 72775247
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=8: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: jcqXR, 1074529204
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=9: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: AkWIO, 1961723427
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=10: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: hBoib, 929089763
2022-12-19 11:01:46,490 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=11: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: GSvzm, 827085798
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=12: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: NNAYI, 94307133
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=13: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: EexFl, 1823689599
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=14: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: CBXUb, 869582787
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=15: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: Wbxtm, 1469371353
2022-12-19 11:01:46,491 INFO  org.apache.seatunnel.connectors.seatunnel.console.sink.ConsoleSinkWriter - subtaskIndex=0 rowIndex=16: SeaTunnelRow#tableId=-1 SeaTunnelRow#kind=INSERT: mIJDt, 995616438
```

### 扩展示例：从 MySQL 到 Doris 批处理模式

#### 步骤1：下载连接器
首先，您需要在`${SEATUNNEL_HOME}/config/plugin_config`文件中加入连接器名称，然后，执行命令来安装连接器(当然，您也可以从 [Apache Maven Repository](https://repo.maven.apache.org/maven2/org/apache/seatunnel/) 手动下载连接器，然后将其移动至`connectors/`目录下)，最后，确认连接器`connector-jdbc`、`connector-doris`在`${SEATUNNEL_HOME}/connectors/`目录下即可。

```bash
# 配置连接器名称
--seatunnel-connectors--
connector-jdbc
connector-doris
--end--
```

```bash
# 安装连接器
sh bin/install-plugin.sh
```

#### 步骤2：放入 MySQL 驱动 

您需要下载 [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 驱动，并放置在 `${SEATUNNEL_HOME}/lib/`目录下

#### 步骤3：添加作业配置文件来定义作业

```bash
cd seatunnel/job/

vim st.conf

env {
  parallelism = 2
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:mysql://localhost:3306/test"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        user = "user"
        password = "pwd"
        table_path = "test.table_name"
        query = "select  * from test.table_name"
    }
}

sink {
   Doris {
          fenodes = "doris_ip:8030"
          username = "user"
          password = "pwd"
          database = "test_db"
          table = "table_name"
          sink.enable-2pc = "true"
          sink.label-prefix = "test-cdc"
          doris.config = {
            format = "json"
            read_json_by_line="true"
          }
      }
}
```

关于配置的更多信息请查看[配置的基本概念](../../introduction/concepts/config.md)

#### 步骤 4: 运行SeaTunnel应用程序

您可以通过以下命令启动应用程序：

```shell
cd seatunnel/
./bin/seatunnel.sh --config ./job/st.conf -m local

```

**查看输出**: 当您运行该命令时，您可以在控制台中看到它的输出。您可以认为这是命令运行成功或失败的标志。

SeaTunnel控制台将会打印一些如下日志信息:

```shell
***********************************************
           Job Statistic Information
***********************************************
Start Time                : 2024-08-13 10:21:49
End Time                  : 2024-08-13 10:21:53
Total Time(s)             :                   4
Total Read Count          :                1000
Total Write Count         :                1000
Total Failed Count        :                   0
***********************************************
```

:::tip

如果您想优化自己的作业，请参照连接器使用文档

:::

## 第二部分：集群部署

如果您已经完成单机验证，并希望在多节点环境中运行 SeaTunnel Engine，请继续阅读[SeaTunnel Engine(Zeta) 安装部署](../../engines/zeta/deployment.md)。

集群部署文档集中说明了以下内容：

- 不同部署模式的适用场景，包括 Local 模式、混合集群模式和分离集群模式
- 混合集群模式与分离集群模式的部署步骤
- 选择部署模式时的建议

建议：

- 如果您只是想在一台机器上快速验证配置和任务链路，使用本页中的 Local 模式即可。
- 如果您需要多节点运行、资源隔离或更贴近测试和生产环境的部署方式，请进入集群部署文档继续操作。

## 下一步

- 如果你想先建立整体路径感，可以返回阅读[快速入门总览](../overview.md)。
- 当你准备把示例 Source 和 Sink 替换成真实连接器时，建议继续阅读[作业配置指南](../job-configuration-guide.md)。
- 开始编写您自己的配置文件，选择您想要使用的[连接器](../../connectors/source)，并根据连接器的文档配置参数。
- 如果您要部署多节点 SeaTunnel Engine 集群，请继续阅读[SeaTunnel Engine(Zeta) 安装部署](../../engines/zeta/deployment.md)。
- 如果您想进一步了解 SeaTunnel Engine，请参阅[SeaTunnel引擎](../../engines/zeta/about.md)。
