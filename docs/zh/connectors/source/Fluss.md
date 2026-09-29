import ChangeLog from '../changelog/connector-fluss.md';

# Fluss

> Fluss Source 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [x] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 描述

Fluss Source 用于在批处理或流处理作业中，从已有的 Fluss 表读取数据。

连接器通过 Fluss 的 log scanner 读取数据，每个表 bucket 对应一个分片（split），因此读取并行度与 bucket 数量一致。每条记录的变更类型（`INSERT`、`UPDATE_BEFORE`、`UPDATE_AFTER`、`DELETE`）会映射为对应的 SeaTunnel `RowKind`，因此日志表以仅追加（append-only）的 `INSERT` 形式读取，主键表则以其变更日志（changelog）的形式读取。

:::caution 主键表

对于主键表，连接器只从最早可用的 log offset 开始读取表的 **changelog**，**不会**先读取 KV 快照。因此它能捕获持续发生的变更（插入/更新/删除）并带上正确的 `RowKind`，但**不保证**对已存在数据的完整初始加载：任何因日志保留（retention）或压缩（compaction）而从 changelog 中被清除的记录都会缺失。目前**尚不支持**主键表的“快照 + 增量”完整同步。如果需要完整的当前状态，请优先使用**日志表（append-only）**，log scanner 总能将其完整读取。

:::

有界性由作业模式决定：

- 在 `BATCH` 模式下，Source 是有界的：每个 bucket 读取到作业启动时捕获的最新 log offset 后，分片结束。
- 在 `STREAMING` 模式下，Source 是无界的：会持续读取新的 log 记录。每个 bucket 的读取位置会保存在 checkpoint 状态中，作业可从中断处恢复。

使用 `start_mode` 选择每个 bucket 的起始读取位点：

- `earliest`（默认）：从最早可用的 offset 开始读取整个 log。
- `latest`：只读取作业启动之后新追加的记录。

`start_mode=latest` 仅对流处理作业有意义。

运行作业前，Fluss database 和 table 必须已经存在。Source 不会自动创建 Fluss database 或 table。表结构会自动从 Fluss 集群读取，因此不需要配置 `schema` 选项。

## 限制

- **仅支持单表。** 每个 source 只读取一张表，通过 `database` + `table` 配置。不支持在一个 source 中读取多张表。
- **不支持指定任意起始位点。** 起始位置只能通过 `start_mode`（`earliest` 或 `latest`）选择；不支持从指定的 log offset 开始读取。
- **不支持分区表。** 将 source 指向分区 Fluss 表会在作业启动时直接失败（fail-fast）并给出错误。请使用非分区表。
- **主键表仅按 changelog 读取。** 连接器读取表的 changelog，而非 KV 快照，因此不做完整初始加载（“快照 + 增量”）。详见上方主键表 caution。

## 依赖

```xml
<dependency>
    <groupId>com.alibaba.fluss</groupId>
    <artifactId>fluss-client</artifactId>
    <version>0.7.0</version>
</dependency>
```

## Source 选项

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
|---|---|---|---|---|
| bootstrap.servers | string | 是 | - | Fluss coordinator 地址，例如 `fluss-coordinator:9123`。 |
| database | string | 是 | - | 要读取的 Fluss database。 |
| table | string | 是 | - | 要读取的 Fluss table。 |
| client.config | map | 否 | - | 传递给 Fluss 连接的额外 Fluss 客户端选项。 |
| start_mode | string | 否 | earliest | 每个 bucket 的起始读取位点：`earliest`（整个 log）或 `latest`（仅作业启动后新追加的记录）。`latest` 在 `BATCH` 模式下会被拒绝。 |
| poll.timeout.ms | long | 否 | 10000 | 单次 Fluss log scanner poll 的最大阻塞时间，单位毫秒。 |
| common-options | - | 否 | - | Source 通用选项，详见 [Source 通用选项](../common-options/source-common-options.md)。 |

### client.config

使用 `client.config` 传递额外的 Fluss 客户端配置。

```hocon
client.config = {
  request.timeout = "30s"
}
```

支持的配置项请参考 Fluss 客户端文档。

## 数据类型映射

| Fluss 数据类型 | SeaTunnel 数据类型 |
|---|---|
| BOOLEAN | BOOLEAN |
| TINYINT | TINYINT |
| SMALLINT | SMALLINT |
| INT | INT |
| BIGINT | BIGINT |
| FLOAT | FLOAT |
| DOUBLE | DOUBLE |
| DECIMAL | DECIMAL |
| CHAR | STRING |
| STRING | STRING |
| BINARY | BYTES |
| BYTES | BYTES |
| DATE | DATE |
| TIME | TIME |
| TIMESTAMP | TIMESTAMP |
| TIMESTAMP_LTZ | TIMESTAMP_TZ |

## 任务示例

### 批处理读取

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Fluss {
    bootstrap.servers = "fluss-coordinator:9123"
    database = "fluss_db"
    table = "fluss_table"
    plugin_output = "fluss_source"
  }
}

sink {
  Console {
    plugin_input = "fluss_source"
  }
}
```

### 流处理读取

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  Fluss {
    bootstrap.servers = "fluss-coordinator:9123"
    database = "fluss_db"
    table = "fluss_table"
    start_mode = "latest"
  }
}

sink {
  Console {
  }
}
```

### 将一张 Fluss 表流式写入另一张 Fluss 表

本示例在流处理模式下把一个 Fluss 源表复制到 Fluss 目标表。Source 从最早可用的
log offset 开始读取，连接器在每次 checkpoint 时提交每个 bucket 的读取位置，
作业重启后可以从断点恢复。

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  Fluss {
    bootstrap.servers = "fluss-coordinator:9123"
    database = "fluss_stream_db"
    table = "fluss_stream_src"
    start_mode = "earliest"
    poll.timeout.ms = 10000
    plugin_output = "fluss_stream"
  }
}

sink {
  Fluss {
    bootstrap.servers = "fluss-coordinator:9123"
    database = "fluss_stream_db"
    table = "fluss_stream_sink"
    plugin_input = "fluss_stream"
  }
}
```

### 为高延迟集群调优 poll 超时

当 Fluss coordinator 位于高延迟网络或返回较大批次时，可以增大 `poll.timeout.ms`，
让 log scanner 在空轮询之间等待更长时间，减少往返次数。

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 60000
}

source {
  Fluss {
    bootstrap.servers = "fluss-coordinator:9123"
    database = "fluss_db"
    table = "fluss_table"
    start_mode = "latest"
    poll.timeout.ms = 60000
    client.config = {
      request.timeout = "30s"
    }
  }
}
```

## Changelog

<ChangeLog />
