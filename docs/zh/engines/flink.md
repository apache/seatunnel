# SeaTunnel 运行在 Flink 上

当你的团队已经在生产中运行 Flink 集群，并希望 SeaTunnel 作业复用这套运行时平台时，Flink 会是一个很合适的选择。如果你只是第一次评估 SeaTunnel，且没有现成的 Flink 运维体系，建议先从 [SeaTunnel Engine](./zeta/about.md) 开始，再根据实际需要回到这一页。

## 从这里开始

如果你的目标是让 SeaTunnel 跑在 Flink 上，建议按下面顺序阅读：

- [引擎概览](./overview.md)
- [Flink 引擎快速开始](../getting-started/locally/quick-start-flink.md)
- [作业配置指南](../getting-started/job-configuration-guide.md)

## 什么时候选择 Flink

以下场景通常更适合使用 Flink：

- 团队已经长期运行 Flink 集群
- 希望复用已有的 Flink 部署、监控与运维体系
- 作业需要融入更大的 Flink 流处理环境

## Flink 专属配置如何写

SeaTunnel 作业里的 Flink 专属配置，需要写在 `env` 块中，并使用 `flink.` 前缀。

示例：

```hocon
env {
  parallelism = 1
  flink.execution.checkpointing.unaligned.enabled = true
}
```

某些枚举类配置项暂时不适合直接内联在 SeaTunnel 作业配置里，遇到这类参数时，建议放到 Flink 自身配置中。当前常见的内联支持类型主要包括：

- `Integer`
- `Boolean`
- `String`
- `Duration`

## 最小示例作业

下面这个例子会在 Flink 上运行，并把生成的数据打印到控制台。

```hocon
env {
  parallelism = 1
  checkpoint.interval = 5000

  flink.execution.checkpointing.mode = "EXACTLY_ONCE"
  flink.execution.checkpointing.timeout = 600000
}

source {
  FakeSource {
    row.num = 16
    plugin_output = "fake_table"
    schema = {
      fields {
        c_map = "map<string, string>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_int = int
        c_bigint = bigint
        c_double = double
        c_bytes = bytes
        c_date = date
        c_decimal = "decimal(33, 18)"
        c_timestamp = timestamp
      }
    }
  }
}

transform {
  FieldMapper {
    plugin_input = "fake_table"
    plugin_output = "fake_output"
    field_mapper = {
      c_string = c_string
      c_int = c_int
    }
  }
}

sink {
  Console {
    plugin_input = "fake_output"
  }
}
```

如果你需要更多 transform 能力，继续查看 [Transforms 目录](../transforms) 和 [Transform 通用参数](../transforms/common-options/common-options.md)。

## 从源码仓库运行示例

如果你是在源码仓库里运行示例，对应模块是：

- `seatunnel-examples/seatunnel-flink-connector-v2-example`

示例入口类是：

- `org.apache.seatunnel.example.flink.v2.SeaTunnelApiExample`

## 下一步

- [Flink 引擎快速开始](../getting-started/locally/quick-start-flink.md)
- [Flink 转换层](../architecture/api-design/flink-translation-layer.md)
- [Transforms 目录](../transforms)
- 如果你还想和默认引擎对比，可回看 [SeaTunnel Engine](./zeta/about.md)
