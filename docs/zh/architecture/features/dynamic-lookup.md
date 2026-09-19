---
sidebar_position: 4
title: 动态 Lookup
---

# 动态 Lookup 架构

## 1. 概述

动态 Lookup 是 SeaTunnel Engine 原生的流式补全动作。它在同一个 SeaTunnel Engine 作业中把
append-only 的事实流与 CDC 维表流关联起来，并输出补全后的结果表。

首个支持范围有意收窄：

- fact 侧必须是 append-only
- dimension 侧必须拒绝主键更新
- schema 变更必须 fail fast
- fact 与 dimension 并行度必须相同
- fact source 只能有一个 dynamic lookup 消费者
- dimension bootstrap 边必须专用于该 lookup
- 运行时依赖 SeaTunnel Engine checkpoint barrier 完成恢复

Dynamic lookup 不是普通 Transform 插件。解析器会创建专用的 `DynamicLookupAction`，因为运行
时需要协调两个 source 输入、source gate 打开时序、checkpoint intent 元数据和维表状态所有权。

## 2. 基本配置

`dynamic_lookup` 与 `source`、`transform`、`sink` 同级声明。

```hocon
env {
  job.mode = "STREAMING"
}

source {
  Kafka {
    plugin_output = "orders_fact"
    topic = "orders"
    # 其他 Kafka 参数。
  }

  MySQL-CDC {
    plugin_output = "customer_dimension"
    # 其他 CDC 参数。
  }
}

dynamic_lookup {
  orders_with_customer {
    uid = "orders_customer_lookup_v1"
    plugin_output = "orders_enriched"

    fact {
      input = "orders_fact"
      key = ["customer_id"]
      changelog-mode = "APPEND_ONLY"
      required-capability = ["FACT_SOURCE_GATE_V1"]
    }

    dimension {
      input = "customer_dimension"
      table = "inventory.customers"
      key = ["id"]
      primary-key-update = "FAIL"
      required-capability = [
        "ORDERED_BOOTSTRAP_V1",
        "ATOMIC_UPDATE_PAIR_V1",
        "PK_UPDATE_REJECT_V1"
      ]
    }

    join {
      type = "LEFT"
      fields = [
        "fact.order_id",
        "fact.customer_id",
        "fact.amount",
        "dimension.name as customer_name",
        "dimension.level as customer_level"
      ]
    }

    schema-change {
      behavior = "FAIL"
    }

    state {
      backend = "IN_MEMORY"
      ttl = "NONE"
      max-concurrent-snapshots = 1
    }

    resource {
      max-logical-state-bytes-per-subtask = "512mb"
      max-resident-state-bytes-per-subtask = "512mb"
      max-concurrent-snapshots = 1
    }
  }
}

sink {
  Console {
    plugin_input = "orders_enriched"
  }
}
```

## 3. Join 语义

Dynamic lookup 支持两种 join 类型：

| `join.type` | 行为 |
|---|---|
| `LEFT` | 每条 fact 记录都会输出。dimension key 不存在时，dimension 字段输出为 null。 |
| `INNER` | 只有找到匹配 dimension 记录的 fact 记录才会输出。 |

投影字段必须使用 `<side>.<field>` 语法，`side` 只能是 `fact` 或 `dimension`。字段别名使用
`as`，例如 `dimension.name as customer_name`。

输出表 schema 由投影字段生成。字段类型、精度、scale 以及其他列元数据从被选择的输入列复制而
来。fact 侧字段的可空性按 fact 列复制；dimension 侧字段在 `LEFT` join 下会变成 nullable，因
为维表行可能不存在；在 `INNER` join 下保留 dimension 列自身的可空性。

`INNER` join 会丢弃没有匹配 dimension key 的 fact 行。运行时会对第一条 miss 和 2 的幂次 miss
计数输出节流 WARN，给后续数据核对留下审计线索，同时避免逐行刷日志。

## 4. 运行时与恢复模型

作业启动时，dimension 流先消费，fact 流暂不打开。fact source gate 会暂存 fact splits，直到
checkpoint 把维表状态与 fact position 作为 durable anchor 记录下来。

checkpoint 期间：

1. fact 与 dimension 输入 barrier 按端口对齐
2. 只有两个端口都到达同一个 checkpoint barrier 后，才 snapshot dimension state
3. completed checkpoint 保存 dynamic lookup intent 元数据
4. fact position 是否 durable 由已提交 checkpoint 内容推导，而不是依赖易失内存回调
5. durable anchor checkpoint 完成后，fact gate 被打开

`uid` 是这个 dynamic lookup operator 的稳定 checkpoint identity。重启或升级作业时应保持不变；
如果修改 `uid`，就相当于创建了一个新的 checkpoint identity，已有 lookup state 可能无法再被该
operator 使用。

恢复时，dynamic lookup state envelope 会通过稳定的 payload length 和 SHA-256 digest 校验后再
使用。普通 completed checkpoint 继续使用 legacy raw payload 格式。只有 dynamic lookup anchor
checkpoint 才使用 versioned completed-checkpoint envelope；没有 envelope 的 completed checkpoint
进入严格 legacy 路径。

## 5. Source 能力要求

fact source 必须声明 `FACT_SOURCE_GATE_V1`。首个实现为 Kafka 接入了该能力。gate 关闭期间，
Kafka splits 会被暂存，并通过 fact gate state envelope 参与 snapshot，不再在恢复时回流到
enumerator restore 路径。durable anchor checkpoint 完成后，engine 发送 open command，暂存
splits 只会被激活一次。

dimension source 必须声明 ordered bootstrap 与 update-pair 能力。CDC incremental source 声明：

- `ORDERED_BOOTSTRAP_V1`
- `ATOMIC_UPDATE_PAIR_V1`
- `PK_UPDATE_REJECT_V1`

dynamic lookup runtime 会强制 `UPDATE_BEFORE` 与 `UPDATE_AFTER` 是同一个 key。主键更新会被当作
作业失败错误处理。

## 6. M0 限制

首个实现会直接拒绝或限制这些场景：

- dimension 侧主键更新
- schema change event
- append-only 以外的 fact changelog mode
- fact 与 dimension 并行度不同
- 同一个 fact source 被多个 dynamic lookup 消费
- 非 dedicated dimension bootstrap edge
- 单个 lookup subtask 超过一个并发 snapshot
- 单个 subtask 的逻辑维表状态超过 512 MiB
- disk-backed dimension state 与 remote staging 预算

如果作业需要分支级 gate、远程多 channel exchange、temporal join、schema evolution 或维表主键
重写，或逻辑状态超过 M0 in-memory 上限，需要使用后续协议版本。
