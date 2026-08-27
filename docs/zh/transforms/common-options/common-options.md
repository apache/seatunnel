---
sidebar_position: 1
title: Transform 通用参数
---

# Transform 通用参数

SeaTunnel 的 Transform 共享一组很小的“编排参数”。这些参数不决定 Transform 本身做什么转换，而是决定它如何与上游、下游数据集连接起来。

## 已废弃的旧参数名

:::caution 警告

旧参数名 `source_table_name` 和 `result_table_name` 已废弃。新的配置请统一使用 `plugin_input` 和 `plugin_output`。

:::

## 共享的编排参数

| 参数 | 含义 | 典型使用场景 |
| --- | --- | --- |
| `plugin_input` | 声明当前 Transform 要消费哪个上游数据集。如果省略，默认读取配置顺序里的前一个插件输出。 | 当你要读取一个命名的中间数据集，或者整条作业不是简单单链路时使用。 |
| `plugin_output` | 把当前 Transform 的结果注册成一个命名数据集，供后续 Transform 或 Sink 引用。 | 当多个下游步骤需要复用同一份结果，或者你想让 pipeline 图更清晰时使用。 |

## 数据集是如何串起来的

从高层看，Transform 的连接方式主要有两种：

- **隐式串联**：每个插件默认读取配置顺序中的前一个插件输出
- **显式命名编排**：通过 `plugin_input` 和 `plugin_output` 引用命名数据集

对于非常短的小作业，隐式串联写起来更快。以下场景更建议使用显式命名：

- 一个 source 要供多个下游步骤复用
- 一个 transform 结果要被多个 sink 消费
- 作业里包含多个逻辑表
- 你希望 pipeline 图在排障时更容易读懂

## 示例：命名数据集流转

下面这个例子里，source 注册了 `fake`，transform 读取它并产出 `fake1`，两个 sink 再分别消费不同的数据集。

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 100
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
        c_timestamp = "timestamp"
      }
    }
  }
}

transform {
  Sql {
    plugin_input = "fake"
    plugin_output = "fake1"
    query = "select id, upper(name) as name, age + 1 as age, c_timestamp from fake"
  }
}

sink {
  Console {
    plugin_input = "fake1"
  }
  Console {
    plugin_input = "fake"
  }
}
```

## 实践建议

- 数据集命名尽量短且语义明确
- 一旦出现分支或多表行为，优先使用显式命名
- 让 transform 文档和配置示例中的数据集名保持一致
- 不要用数据集编排去掩盖过于复杂的逻辑；当链路已经难以理解时，应考虑拆分作业

## 相关文档

- [Transform 插件体系](../../architecture/transform-plugin-system.md)
- [作业配置指南](../../getting-started/job-configuration-guide.md)
- [Transforms 目录](..)
