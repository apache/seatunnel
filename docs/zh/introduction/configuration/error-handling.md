---
title: 错误数据处理
---

# 错误数据处理（实验性功能）

在 SeaTunnel 中，默认行为：只要某个 Connector 或 Transform 抛出异常，**整个作业就会失败**。

从本实验性能力开始，用户可以改变这一行为，由引擎 **捕获异常数据，将其路由到错误 Sink，并在条件允许时继续推进作业**。

> **状态：实验性（Experimental）**
>
> 目前该能力只接入 Zeta 引擎，且经过验证的 Sink 实现为 JDBC Sink。Flink/Spark translation 路径暂未使用这套机制。错误处理与行级错误路由默认关闭，配置和语义在后续版本中可能有调整。

## 适用场景

推荐启用错误处理的典型场景包括但不限于：

- 大批量离线任务中存在少量脏数据（例如非法时间、字符串超长等）；
- Sink 表偶发出现主键或唯一约束冲突；
- 需要在存在个别异常记录的情况下保持作业整体可用性，并将错误数据单独记录以便后续排查和补数。

不建议或需谨慎启用错误处理的场景包括：

- 对“所有合法数据必须严格写入”具有较强 at-least-once 或 exactly-once 语义要求；
- 使用复杂的多表 Sink 并希望在多个表之间保持严格一致性语义的场景。

## 快速上手

对大多数用户来说，最简单的试用方式是：只在 JDBC Sink 上开启该能力，并把失败的数据写入一张单独的 JDBC 错误表。

1. 准备正常业务表和单独的错误表。错误表字段可参考[错误表结构](#错误表结构)。
2. 保持原有正常 Sink 配置不变。
3. 增加 `env.sink_error_handler`，并设置 `mode = "ROUTE"`。
4. 在 `env.sink_error_handler.sink` 下配置错误 Sink。
5. 使用 Zeta Engine 提交作业，作业运行后查看错误表。

最小示例：

```hocon
env {
  sink_error_handler {
    mode = "ROUTE"
    max_error_records = 10

    sink {
      plugin_name = "Jdbc"
      url = "jdbc:mysql://localhost:3306/test"
      driver = "com.mysql.cj.jdbc.Driver"
      user = "root"
      password = "******"
      error_table = "orders_error"
    }
  }
}
```

实际效果可以理解为：

- 正常数据继续写入原来的 Sink。
- 被判断为行级错误的数据会写入 `orders_error`。
- 同一个作业里的同一个 Sink 阶段处理的行级错误超过 10 条时，作业失败。
- 连接失败等系统级错误仍然会导致作业失败。

如果您只是想先观察行级错误，可以把 `mode = "ROUTE"` 改成 `mode = "LOG"`。`LOG` 模式只会把错误信息写入日志，不会写入错误表。

## 整体思路

启用错误处理后，Zeta 引擎对于每条记录的处理逻辑可概括为：

1. 首先按照原有逻辑，由 Transform / Sink 正常处理该记录；
2. 在处理过程中如发生异常，引擎会尝试区分：
   - **行级错误**：由于该条数据本身引起的异常（例如数据格式错误、约束冲突等）；
   - **系统级错误**：例如连接中断、资源不足（OOM）等基础设施问题；
3. 对于系统级错误，行为与默认一致：直接失败作业；
4. 对于被判定为行级错误的情况，引擎会将该记录及异常信息交给 **错误处理器（ErrorHandler）**：
   - `mode = LOG`：仅记录日志；
   - `mode = ROUTE`：在记录日志的基础上，将错误记录写入单独配置的 **错误 Sink**（例如 JDBC 错误表）。

其余正常记录仍会沿原有链路向下游传递。

错误处理行为通过 **env 配置** 控制：

- **阶段级（env）**：在 `env.transform_error_handler` / `env.sink_error_handler` 中统一配置该阶段默认行为；
- **全局（env）**：在 `env.error_handler` 中给所有阶段提供默认值。

部分 Transform（例如 JsonPath、DataValidator）仍然保留自身早期的 `row_error_handle_way` 等行错误控制选项，这些选项与本文介绍的引擎级错误处理机制可以并行存在，但目前尚未与 `env.*_error_handler` 做自动合并。

## 核心概念

### 模式（mode）

配置中最常见的字段为 `mode`：

- `DISABLE`：关闭该阶段的错误处理（默认行为）；
- `LOG`：仅记录行级错误日志，不路由到错误 Sink；
- `ROUTE`：记录并将行级错误路由到错误 Sink。

如果完全不配置上述选项，SeaTunnel 的行为与历史版本保持一致：任意异常都会导致作业失败。

### 错误 Sink

**错误 Sink** 是专门用于接收错误数据的一条 Sink，需要在 `..._error_handler.sink` 下进行配置，例如：

```hocon
env {
  sink_error_handler {
    mode = "ROUTE"

    sink {
      plugin_name = "Jdbc"
      error_table = "orders_sink_error_basic"
      # 这里配置错误表对应的 Jdbc Sink 选项
    }
  }
}
```

一种常见用法是：

- 主 Sink 写入业务表（例如 `orders_from_sink`）；
- 错误 Sink 写入错误表（例如 `orders_sink_error_*`），用于后续排查和补数。

### 行级错误 vs 系统级错误

在大多数情况下，用户无需手动编写逻辑来判断“是否为行级错误”。

引擎会尝试区分：

- **行级错误**：通常由单条数据本身导致，引擎可在配置允许时旁路该条数据并继续作业；
- **系统级错误**：通常是连接中断、资源不足（OOM）等基础设施问题，会直接导致作业失败。

当前版本的默认分类策略：

- **Sink 阶段**：若 Sink Connector 未实现 `SupportRowLevelErrorClassifier`，其异常将被当作系统级错误处理（即使配置了 `sink_error_handler` 也会失败作业）。
- **Transform 阶段**：若 Transform 未实现 `SupportRowLevelErrorClassifier`，其异常将被当作系统级错误处理（即使配置了 `transform_error_handler` 也会失败作业）。

对于部分 Connector（例如 JDBC），Connector 本身会通过接口显式声明“哪些异常属于行级错误”。引擎会优先采用这类显式声明。

只有实现了 `SupportRowLevelErrorClassifier` 的 Connector/Transform，才能触发行级错误；否则所有异常都会被当作系统级错误处理并导致作业失败。

> 说明
>
> 本文描述的是当前版本的 Zeta 引擎处理流程。后续会逐步推动更多内置 Transform 和引擎集成实现 `SupportRowLevelErrorClassifier`，以便更准确地区分“行级错误”与“系统级错误”。

### ROUTE 模式的可靠性范围

在 Zeta 中，`ROUTE` 模式会在 checkpoint ack 前等待待写入的错误记录写入完成，并 flush 已配置的错误 Sink writer。若错误 Sink 写入或关闭失败，任务会失败，而不是仅记录日志后表现为正常关闭。

当前实验性实现仅支持不需要 writer state、committer、aggregated committer 或 commit-info serializer 的错误 Sink。若配置的错误 Sink 启用了这类生命周期能力，作业会在初始化阶段快速失败，而不是在 checkpoint/commit 语义不完整的情况下继续运行。例如，JDBC 错误 Sink 在 `ROUTE` 模式下不应开启 exactly-once/XA 相关选项。

该能力仍属于实验性功能。错误记录的最终投递语义取决于所配置错误 Sink Connector 自身的事务和提交行为，因此不应将其视为通用的 exactly-once DLQ 保证。

### Transform 阶段发生行级错误时会怎样

当 Transform 被判定为行级错误时，**该条记录会从主链路中被丢弃**，不会进入后续 Transform，也不会进入下游 Sink：

- 对 `map(...)`：返回 `null`，等价于“过滤掉该条记录”；
- 对 `flatMap(...)`：返回空列表，等价于“丢弃该条记录”。

如果同时开启了 `mode = ROUTE` 且配置了错误 Sink，则该条原始记录及异常信息仍可被写入错误表用于排查和补数。

## 配置与参数说明

### 配置位置

错误处理目前主要通过 **env 配置** 生效：

- **阶段级（env）**：在 `env.transform_error_handler` / `env.sink_error_handler` 中统一配置该阶段默认行为，例如：

  ```hocon
  env {
    transform_error_handler {
      mode = "ROUTE"

      sink {
        plugin_name = "Jdbc"
        error_table = "orders_transform_error_from_env"
      }
    }

    sink_error_handler {
      mode = "ROUTE"
      queue_capacity = 10000
      queue_overflow_policy = "FAIL"

      sink {
        plugin_name = "Jdbc"
        error_table = "orders_sink_error_from_env"
      }
    }
  }
  ```

- **全局（env）**：在 `env.error_handler` 中为所有阶段提供默认值，例如：

  ```hocon
  env {
    error_handler {
      mode = "LOG"
      include_original_data = true
      include_stacktrace = false
    }
  }
  ```

同名参数的覆盖顺序（由高到低）：

1. 阶段级 `env.transform_error_handler` / `env.sink_error_handler`；
2. 全局 `env.error_handler`（默认 `DISABLE`）。

各个 Transform / Sink 插件自身已有的错误处理选项（例如 JsonPath / DataValidator 的 `row_error_handle_way`）目前与上述 env 配置**相互独立**：插件内部选项仅影响该插件内部行为，而 `env.*_error_handler` 控制的是引擎级的行级错误旁路能力。

### 通用参数一览

| 参数                     | 类型     | 默认值   | 说明 / 取值                                                                                 |
|------------------------|--------|--------|------------------------------------------------------------------------------------------|
| `mode`                 | String    | `DISABLE` | 行级错误处理模式：`DISABLE`（关闭）、`LOG`（只记录）、`ROUTE`（记录并路由到错误 Sink）。不支持的取值会在解析配置时快速失败。                         |
| `max_error_ratio`      | Double     | `0.0`   | 允许的错误比例，0.0–1.0；例如 `0.01` 表示错误记录超过 1% 时失败作业；`0.0` 表示不按比例触发失败。超出 0.0–1.0 的取值会在解析配置时快速失败。                          |
| `max_error_ratio_min_records` | Integer | `10000`  | `max_error_ratio` 的预热阈值：当总处理记录数小于该值时，不进行比例触发，避免在处理记录数还不够时过早失败。 |
| `max_error_records`    | Long    | `0`     | 允许的错误记录总数上限；`0` 表示不按错误条数触发失败。负数会在解析配置时快速失败。                                                           |
| `queue_capacity`       | Integer     | `10000` | 内部错误队列（缓冲区）容量上限，队列中最多可同时缓存的错误记录数量。                                                     |
| `queue_overflow_policy`| String    | `FAIL`  | 错误队列已满时的策略：`FAIL`（失败作业）、`DROP`（丢弃新错误记录）、`BLOCK`（阻塞生产错误的线程，可能影响吞吐）。不支持的取值会在解析配置时快速失败。                 |
| `include_original_data`| Boolean    | `false` | 是否在错误记录中包含原始数据内容。                                             |
| `include_stacktrace`   | Boolean    | `false` | 是否在错误记录中包含完整 Java 异常堆栈；开启会增加单条错误记录的体积。                                                |
| `original_data_format` | String    | `TEXT`  | **预留参数**。当前版本仅支持 `TEXT`，内部统一按字符串形式写入错误表（`original_data` 为记录的字符串表示，即 `String.valueOf(row)`）。不支持的取值会在解析配置时快速失败。 |
| `original_data_max_length` | Integer | `8192`  | 原始数据序列化后的最大长度，超过部分将被截断，用于控制单条错误记录大小。                                                |

阈值统计口径：Zeta 会把阈值计数写入引擎状态，计数 key 带有版本号，并按作业 ID、pipeline ID、action ID 和阶段（`TRANSFORM` 或 `SINK`）划分。行处理时会立即更新这组共享的引擎状态计数器，所以同一个 action、同一个阶段下的所有并行 subtask 都能看到同一组当前总记录数/错误记录数计数器。因此 `max_error_records` 和 `max_error_ratio` 按该作业、该 pipeline 的阶段级总量触发，而不是给每个 subtask 单独分配一份预算。Sink 每次 `write(...)` 计 1；Transform 链中每个 `map(...)`/`flatMap(...)` 调用计 1；同一条 Transform 链上的多个算子共享同一个阶段计数器。不同 action、不同阶段分别使用不同计数器。任务恢复后，新 attempt 会复用同一组引擎状态计数器，不会从 0 重新计数，所以重启或调整并行度不会放大可容忍的错误数量。

### 错误 Sink 相关参数一览

在 `..._error_handler.sink` 下配置错误记录要写到哪里：

| 参数            | 类型   | 说明                                                                 |
|-----------------|------|--------------------------------------------------------------------|
| `plugin_name`   | String | 错误 Sink 使用的 Connector 名称，例如 `Jdbc`。                               |
| `error_table`   | String | （JDBC 专用）错误记录要写入的目标表名，例如 `orders_sink_error_basic`。             |

除此之外，错误 Sink 还需要配置各自 Connector 的常规参数，例如 JDBC 的 `url`、`username`、`password`、`driver` 等，写法与普通 Sink 完全一致。

如果 `mode = ROUTE`，必须配置 `sink.plugin_name`。当缺少 `sink { ... }` 或 `plugin_name` 为空时，作业会在配置解析阶段快速失败，因为没有可接收路由错误记录的错误 Sink。

### 错误表结构

当前引擎为错误 Sink 构造了一张统一的错误表 Schema（以 JDBC 为例）：

- `error_stage`：字符串，错误发生的阶段（例如 `TRANSFORM` / `SINK`）；
- `plugin_type`：字符串，插件类型（例如 `TRANSFORM` / `SINK`）；
- `plugin_name`：字符串，插件名称（例如 `Jdbc` 等）；
- `source_table_path`：字符串，源表路径或标识；
- `job_id`：长整型，SeaTunnel 作业 ID；多个作业共用同一错误表时，可用它区分错误数据来源；
- `error_message`：字符串，异常的简要错误信息（已按照内部上限截断）；
- `exception_class`：字符串，异常类名；
- `stacktrace`：字符串，完整堆栈信息（仅在 `include_stacktrace = true` 时填写）；
- `original_data`：字符串，原始数据内容（仅在 `include_original_data = true` 时填写，长度受 `original_data_max_length` 控制）；
- `occur_time`：时间戳，错误发生时间（UTC）。

上述字段名称在不同错误表中保持一致，便于统一查询和分析。

## JDBC 错误处理如何工作（重点）

JDBC 是当前最主要使用行级错误处理能力的 Connector。

### JDBC 里什么算“行级错误”？

`JdbcSinkWriter` 会检查 `SQLException` 链，如果发现：

- `SQLState` 以 `22` 开头——数据异常（比如数据太长、类型不匹配）；
- `SQLState` 以 `23` 开头——完整性约束异常（比如主键/唯一键冲突）；

在启用 Sink 行级错误处理时，会将其视为 **行级错误**。在该模式下，`22`/`23` SQLState 失败会立即退出 JDBC 重试循环，并把当前 batch 交给错误处理器。未启用 Sink 行级错误处理时，JDBC 仍保持普通的 `max_retries` 重试行为，写入失败也会继续对作业可见。其他 SQL 失败会被视为 **系统级错误**，直接让作业失败。

对于其他 Sink，如果未实现 `SupportRowLevelErrorClassifier` 接口，引擎会更保守地将异常视为系统级错误：即使配置了 `sink_error_handler`，这类异常也不会被当作行级错误旁路，而是直接失败作业。

### 发生行级错误时，批处理会怎样？

JDBC Sink 通常会把多条记录放在一个 JDBC batch 里，一次性发送给数据库。

当写入某条记录时发生了**行级错误**：

- Connector 会捕获这个异常；
- 如果判断这是“行级数据错误”，会调用一个帮助方法，**清空当前内存中的 JDBC batch**。

这意味着：

- 当前 batch 中所有“还没有真正发到数据库、但已经加入 batch 的记录”都会被一起清空；
- 这条坏记录会被交给错误处理器（写日志 / 写错误表）；
- 同一批次中的其它“好记录”**不会被自动重试**。

从使用者角度可以理解为：

> **一旦这个批次中出现行级错误，这整个批次就被当作“错误批次”处理。**

因此，在“**启用了 batch 且启用了错误处理**”的组合下：

- 可能存在极少量原本合法的记录由于与错误数据处于同一批次而未写入目标库；
- 对“所有合法记录”的严格 at-least-once 语义，在此配置组合下不再具备正式保证。

上述行为属于 Connector 级别的当前实现细节，后续会逐步对不同 Sink 的实现进行优化，针对错误的批量提交进行优化，找出具体错误数据进行错误处理，以降低误伤合法记录的概率并提升可追溯性。

### JDBC 使用建议

- 若更关注作业稳定性，并能够接受少量合法记录在错误批次中被丢弃：
  - 可以启用错误处理并保留批写入；
  - 可通过错误表和日志对异常数据进行事后分析与补数。

- 若对“任何合法记录都不得丢失”有严格要求：
  - 可考虑关闭 JDBC 行级错误处理，或
  - 在启用错误处理的同时将 `batch_size` 调小（甚至设置为 `1`），使每个 batch 最多仅包含一条记录；
  - 强烈建议在测试环境中结合实际数据库和 JDBC 驱动充分验证后，再在生产环境中启用该能力。

## 多表 Sink 的当前状态

> **实验性能力，尚未完全支持。**

## 基本配置示例（单表 JDBC Sink）

下面给出一个最小示例，用于演示如何在 Sink 阶段将行级错误路由到 JDBC 错误表：

```hocon
env {
  sink_error_handler {
    mode = "ROUTE"              # 或 LOG / DISABLE
    max_error_ratio = 0.01       # 错误比例 > 1% 时失败作业
    max_error_records = 1000     # 或错误总数 > 1000 时失败作业
    queue_capacity = 10000
    queue_overflow_policy = "FAIL"  # FAIL / DROP / BLOCK

    include_original_data = true
    include_stacktrace = false
    original_data_format = "TEXT"
    original_data_max_length = 8192

    sink {
      plugin_name = "Jdbc"
      error_table = "orders_sink_error_basic"
      # 这里配置错误表对应的 Jdbc Sink 选项
    }
  }
}
```

### MySQL 错误表结构

当 JDBC 错误 Sink 使用默认 save-mode 设置时，SeaTunnel 会根据内置错误表结构自动创建错误表。如果您关闭了自动建表，或需要提前手动建表，可使用如下结构：

```sql
CREATE TABLE sink_error_basic (
    error_stage VARCHAR(50),
    plugin_type VARCHAR(50),
    plugin_name VARCHAR(100),
    source_table_path VARCHAR(255),
    job_id BIGINT,
    error_message TEXT,
    exception_class VARCHAR(255),
    stacktrace TEXT,
    original_data TEXT,
    occur_time TIMESTAMP
);
```

对于 Transform 阶段，可以通过 `transform_error_handler` 进行类似配置。
