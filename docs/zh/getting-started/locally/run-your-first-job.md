---
sidebar_position: 1
title: 跑第一个任务
---

# 跑第一个任务

这一页只解决一件事：用最短路径把 SeaTunnel 真正跑起来。这个示例完全本地运行，不依赖 MySQL、Kafka 或对象存储，适合先确认安装、配置解析和执行引擎都正常。

## 步骤 1：先完成本地部署

先完成 [部署](deployment.md)，并确认 SeaTunnel 目录下已经有 `bin/seatunnel.sh`。

## 步骤 2：只安装这篇示例真正需要的插件

先看 [部署 > 下载连接器插件](deployment.md#下载连接器插件)，然后把 `config/plugin_config` 收敛成下面两个插件：

```plugin_config
--seatunnel-connectors--
connector-fake
connector-console
--end--
```

接着执行安装命令，并确认插件已经下载到 `${SEATUNNEL_HOME}/connectors`：

```bash
cd "${SEATUNNEL_HOME}"
sh bin/install-plugin.sh
ls connectors | rg 'connector-(fake|console)'
```

## 步骤 3：使用最小可运行配置

把下面的配置保存为 `config/v2.batch.config.template`，或者保存为你自己的本地配置文件：

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

## 步骤 4：用本地模式运行

```shell
cd "apache-seatunnel-${version}"
./bin/seatunnel.sh --config ./config/v2.batch.config.template -m local
```

## 验证结果

- 任务可以正常启动，没有 connector 加载错误。
- 控制台会打印映射后字段的 `output rowType` 行。
- 控制台会打印 16 行 `ConsoleSinkWriter` 输出。
- 批任务在写完全部数据后正常退出。

如果这里已经跑通，说明本地基础链路是正常的，后面就可以切到真实数据源和真实目标端。

## 下一步

- 如果你想看完整本地链路，请继续看 [SeaTunnel 引擎快速开始](quick-start-seatunnel-engine.md)。
- 如果你想直接看真实源端到目标端示例，请从下面这些教程开始：
  - [MySQL CDC 到 Doris](../recipes/mysql-cdc-to-doris.md)
  - [JDBC 到 S3](../recipes/jdbc-to-s3.md)
  - [Kafka 到 Iceberg](../recipes/kafka-to-iceberg.md)
  - [Http 到 JDBC](../recipes/http-to-jdbc.md)
  - [File 到 StarRocks](../recipes/file-to-starrocks.md)
  - [多表 CDC](../recipes/multi-table-cdc.md)
