<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# 脏数据收集与校验

脏数据处理为可选能力。不配置 `dirty.collector` 和 `dirty.validator` 时，已有 Sink
行为保持不变。可选校验器可在行进入 Sink 前拒绝不符合规则的 `SeaTunnelRow`，并将这条
确定的记录交给收集器。所有 delegate 写入异常均继续使写入失败，包括已分类的行级错误：
批量写入错误可能来自更早缓存的记录。只配置收集器时不会拦截 Sink 异常；能自行准确归属
错误记录的 Sink 可显式调用收集器 API。

## 配置与优先级

两个配置块均可放在 `env` 或具体 Sink 中。Sink 的 `dirty.collector` 会整体替换 env
中的收集器配置，不逐字段合并。校验器独立继承：Sink 没有 `dirty.validator` 时使用
env 的校验器配置。Sink 显式将某个配置块设为 null 时，禁用对应的 env 继承配置。
未知类型或初始化失败会明确报错，Flink starter 也遵守此规则。
未配置收集器时使用 `noop`；已配置的收集器块必须包含非空 `type`。配置校验器时必须
提供 `type`，且必须配置有效收集器。校验器执行失败会使作业失败，不会静默放行记录。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
  dirty.collector {
    type = "log"
    threshold = 1
    fail_on_threshold = true
    include_stack_trace = false
    log_payload = "false"
  }
}
source {
  FakeSource {
    row.num = 5
    schema.fields { id = int, name = string }
  }
}
sink {
  Console {}
}
```

此示例在收集器接收到脏数据时失败。它没有把所有行标记为脏数据：源数据有效，且未配置
校验器。

## 内置收集器

`type = "noop"`（别名 `none`）不执行收集；`type = "log"` 记录脏数据日志并维护收集器
实例计数。默认计数器属于当前实例，`threshold` 按收集器/任务计算，不是 Zeta、Flink
或 Spark 整个作业统一执行的全局阈值，也不随 checkpoint 持久化。重启或重试可能重置
或重放计数及收集副作用；需要持久恢复和去重保证时，由自定义收集器实现。

| log 收集器参数 | 类型 | 默认值 | 含义 |
| --- | --- | --- | --- |
| `log_level` | string | `ERROR` | 支持 `ERROR`、`WARN`、`INFO`、`DEBUG`，其他值按 ERROR 处理。 |
| `log_first_n_records` | long | `10` | 每个收集器记录前 N 条脏数据，必须大于等于零。 |
| `log_every_n_records` | long | `1000` | 同时记录每第 N 条脏数据，必须为正数；设为 `1` 明确启用逐条日志。 |
| `include_stack_trace` | boolean | `true` | 在日志消息中包含异常详情。 |
| `threshold` | long | `-1` | 触发阈值动作的计数；小于等于零时禁用。 |
| `fail_on_threshold` | boolean | `false` | 达到或超过正数阈值时抛出异常，否则每个收集器实例最多记录一次警告。 |
| `log_payload` | string | `false` | `false` 仅记录字段数，`fields` 记录字段名，`full` 记录值。 |
| `mask_fields` | 字符串列表 | `[]` | full 模式下需要掩码的字段名，忽略大小写。 |

log 收集器默认只输出前 10 条及之后每第 1000 条记录的诊断日志，采样在每个收集器实例
内独立执行。所有记录仍逐条增加计数并判断阈值，包括未采样的记录和配置日志级别未启用
的记录。只有日志级别已启用的采样记录才格式化 payload 和异常信息。关闭时，收集器按
配置级别最多输出一次已观察、已记录和已抑制条数的汇总。需要逐条诊断时，可明确设置
`log_every_n_records = 1`。

不能出现在日志中的数据应保持关闭 payload 日志。如明确启用 full 模式，应配置所需字段
掩码。校验器必须配合有效收集器使用，不能用 `noop` 静默丢弃拒绝记录；请根据所需
处置方式和投递保证配置收集器。

## 自定义 SPI

收集器通过 `DirtyRecordCollectorProvider` 的 `getType` 和 `createCollector` 提供可序列化
的 `DirtyRecordCollector`。校验器实现 `DirtyDataValidator`，在
`validate(row, catalogTable)` 中返回 `ValidationResult.clean()` 或脏数据结果。
将 provider 和 validator 的实现类名登记到以下服务文件：

```text
META-INF/services/org.apache.seatunnel.api.table.factory.Factory
```

初始化发生在作业规划阶段，随后将策略序列化到 worker。反序列化后不会调用 worker 端
初始化回调。策略状态必须可序列化；worker 所需资源应在使用时延迟创建，不能依赖规划
实例打开的临时活跃资源。每个实例必须实现其所拥有资源的关闭逻辑。

实现和服务文件打包为 JAR，并确保作业规划端及引擎 worker/translation 类加载器均可访问。
收集器使用 provider 的类型名选择；校验器使用 factory identifier 或类名选择：

```hocon
sink {
  Console {
    dirty.collector { type = "log", threshold = 1, fail_on_threshold = true }
    dirty.validator { type = "com.example.RequiredFieldsValidator" }
  }
}
```

使用此配置前必须安装自定义校验器 JAR。测试场景中的 `AlwaysDirtyDataValidator` 和计数
收集器为演示工具；前者会拒绝所有行，不能作为生产校验规则使用，二者均不会被默认选择。
计数收集器不格式化记录内容或错误文本，只在关闭时输出一次计数汇总，并最多记录一次
非致命阈值警告。致命阈值在每次达到或超过阈值的调用中均继续抛出异常。

## 运行边界

包装器位于 Zeta、Flink、Spark 的真实 Sink writer 外层，仅支持单表输入及同步单表 writer。
启用脏数据处理时，规划阶段会拒绝多个输入 CatalogTable，包括逐表 sink template 回退
场景；配置和输入限制的校验先于 Sink 准备和 save-mode 副作用。
Zeta、Flink、Spark 会在规划阶段拒绝支持多表的 Sink（包括仅有一个输入表的 JDBC）
及已聚合的多表 Sink，检查发生在调用对应 save-mode handler 之前。启用脏数据处理时，
异步 writer、延迟终态上报和已启用的 Zeta sink error-handler 策略仍在 writer 启动时被
明确拒绝，不会静默降级；多表 writer 的运行时保护检查也继续保留。未启用脏数据处理时，
以上已有链路保持原有行为。写入前校验不会绕过 writer 能力限制。

校验发生在 `write` 前，Zeta 只将此包装器明确校验拒绝的记录上报为 DROPPED。
delegate 正常返回时保留既有的写入接收计数语义，后续 flush、checkpoint 或提交仍可能失败。
自定义 Sink 显式调用收集器只表示诊断或计数，不能据此判断该行的最终处置，也不会自动
产生 DROPPED 指标。所有 delegate 写入异常原样传播，因此 JDBC 批量或已锁存的 flush
失败不会将当前健康记录错误收集。支持模式演进的 delegate 仍保留其模式回调和物理下游表标识。提交、checkpoint、模式变更和关闭阶段的
异常仍按既有生命周期传播。包装器转发状态与提交调用，并随 writer 关闭收集器，不提供
持久化错误存储和重放。需要持久保存拒绝记录时应使用自定义收集器，并单独验证其投递和
恢复保证。
