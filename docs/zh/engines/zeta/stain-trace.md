# 染色追踪（StainTrace）

## 概述

StainTrace 是 SeaTunnel 的数据血缘与端到端性能追踪系统，用于追踪 Zeta 引擎中数据的完整流转过程。

## 核心特性

- **框架级实现**：所有 Connector 自动支持，无需修改连接器代码
- **6 个基础阶段**：SOURCE_EMIT、QUEUE_IN、QUEUE_OUT、TRANSFORM_IN、TRANSFORM_OUT、SINK_WRITE_DONE（已全部落点；实际出现顺序取决于流水线拓扑）
- **扩展阶段**：40+ 个细粒度阶段代码已定义，用于未来精细化性能分析（**当前尚未落点，不会出现在追踪文件中**）
- **本地文件存储**：零依赖，OTLP JSON Lines 格式
- **性能优化**：合理采样配置下开销 < 2%

## 追踪阶段

### 基础阶段（1-6）

这 6 个阶段是**当前唯一有效的追踪阶段**，每条采样记录都会包含这些事件。

> **注意**：阶段在 Trace 中的实际出现顺序取决于流水线拓扑。若 Transform 与 Source 任务融合（transform-before-queue），则顺序为 `SOURCE_EMIT → TRANSFORM_IN → TRANSFORM_OUT → QUEUE_IN → QUEUE_OUT → SINK_WRITE_DONE`；若 Transform 与 Sink 任务融合，则顺序为 `SOURCE_EMIT → QUEUE_IN → QUEUE_OUT → TRANSFORM_IN → TRANSFORM_OUT → SINK_WRITE_DONE`。

| 阶段代码 | 名称 | 说明 |
|---------|------|------|
| 1 | SOURCE_EMIT (S0) | Source 发出数据 |
| 2 | QUEUE_IN (Q+) | 数据进入队列 |
| 3 | QUEUE_OUT (Q-) | 数据离开队列 |
| 4 | TRANSFORM_IN (T+) | Transform 接收数据 |
| 5 | TRANSFORM_OUT (T-) | Transform 输出数据 |
| 6 | SINK_WRITE_DONE (W!) | Sink 写入完成 |

### 性能阶段（101-110）

> ⚠️ **计划中，尚未落点。** 这些阶段代码已在 `StainTraceStage` 中定义，但生产代码中尚无调用点，追踪文件中**不会出现**这些事件。

- SOURCE_READ_END (101)
- QUEUE_OFFER_START (102)
- QUEUE_DESERIALIZE_END (103)
- TRANSFORM_EXECUTE_START/END (104-105)
- SINK_BATCH_AGGREGATE_END (106)
- SINK_FORMAT_END (107)
- SINK_WRITE_START/END (108-109)
- SINK_COMMIT_END (110)

### 细粒度阶段（201-220）

> ⚠️ **计划中，尚未落点。**

- Source：READ_START (201)、SERIALIZE_START/END (202-203)
- Queue：DESERIALIZE_START (204)
- Transform：PARSE_START/END (205-206)、BUILD_START/END (207-208)
- Sink：RECEIVE (209)、BATCH_AGGREGATE_START/END (210)、FORMAT_START/END (211)、COMMIT_START/END (212)
- Checkpoint：SNAPSHOT_START/END (213-214)、BARRIER_EMIT/RECEIVE (215-216)
- 网络传输（仅多节点）：RECORD_SERIALIZE_START/END (217-218)、RECORD_DESERIALIZE_START/END (219-220)

### 流控阶段（226-227）

> ⚠️ **计划中，尚未落点。**

- FLOW_CONTROL_AUDIT_START/END (226-227)：反压检测

## 快速开始

### 1. 配置引擎

编辑 `seatunnel.yaml`：

```yaml
seatunnel:
  engine:
    stain-trace-enabled: true
    stain-trace-sample-interval: 100000  # 每 10 万条采样 1 条
    stain-trace-file-base-path: /data/seatunnel/traces  # 本地追踪文件目录，必须显式配置，默认不启用
```

### 2. 启用任务级开关

编辑作业配置文件：

```hocon
env {
  stain_trace {
    enabled = true
  }
}
```

**重要**：引擎级和任务级开关都必须启用。

### 3. 运行作业

执行 SeaTunnel 作业。只有显式配置 `stain-trace-file-base-path` 后，才会生成追踪文件。

### 4. 查看追踪数据

```bash
# 查看生成的文件
ls -lh /tmp/seatunnel/traces/traces/{job_id}/{date}/

# 查看追踪数据（JSON Lines 格式）
cat /tmp/seatunnel/traces/traces/{job_id}/{date}/traces-*.jsonl | jq .
```

### 5. 生成分析报告

将 `analyze-traces.sh` 与 `seatunnel-trace-analyzer-*-jar-with-dependencies.jar` 放在同一目录下，然后运行：

```bash
./analyze-traces.sh /tmp/seatunnel/traces report.html
open report.html
```

## 配置参考

### 引擎级配置

| 参数 | 类型 | 默认值 | 说明 |
|-----|------|--------|------|
| stain-trace-enabled | boolean | false | 启用追踪的主开关 |
| stain-trace-sample-interval | int | 100000 | 每 N 条记录采样 1 条 |
| stain-trace-max-traces-per-second-per-worker | int | 50 | 每个 Worker 每秒最大追踪数 |
| stain-trace-max-entries-per-trace | int | 32 | 每条追踪最大阶段条目数 |
| stain-trace-propagate-to-all-splits | boolean | false | 是否传播到所有分裂输出 |
| stain-trace-file-base-path | string | 无 | 文件存储根目录。未显式配置该路径时，本地文件写入保持关闭 |
| stain-trace-file-max-events-per-file | int | 10000 | 每个文件最大事件数 |
| stain-trace-file-max-size-mb | int | 10 | 文件最大大小（MB） |
| stain-trace-file-flush-interval-seconds | int | 10 | 刷盘间隔（秒） |

> **注意 - 文件路径与 Checkpoint 的一致性**：建议将 `stain-trace-file-base-path` 与 `checkpoint.storage.plugin-config.namespace` 配置在同一存储根路径下。例如，若 Checkpoint 使用 `/data/seatunnel/checkpoint_snapshot/`，则建议将 Trace 路径配置为 `/data/seatunnel/traces`。在生产环境使用 HDFS 时，两者均应指向同一 HDFS 路径前缀，以保证存储一致性。
>
> ```yaml
> seatunnel:
>   engine:
>     stain-trace-file-base-path: /data/seatunnel/traces
>     checkpoint:
>       storage:
>         type: hdfs
>         plugin-config:
>           namespace: /data/seatunnel/checkpoint_snapshot/
>           storage.type: hdfs
>           fs.defaultFS: hdfs://namenode:9000
> ```

### 任务级配置

```hocon
env {
  stain_trace {
    enabled = true                # 任务级开关
    sample_interval = 1000        # 可选：覆盖引擎级配置
  }
}
```

## 文件格式

### 目录结构

```
/tmp/seatunnel/traces/
└── traces/
    └── {job_id}/
        └── {yyyy-MM-dd}/
            ├── traces-14-30-00-a1b2c3d4.jsonl
            └── traces-14-30-10-e5f6g7h8.jsonl
```

文件名格式：`traces-{HH-mm-ss}-{uuid前8位}.jsonl`

### JSON Lines 格式

每行是一个完整的 OTLP `ExportTraceServiceRequest` JSON 对象（每条采样记录对应一个 span）：

```json
{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"seatunnel"}},{"key":"seatunnel.job_id","value":{"stringValue":"123456"}}]},"scopeSpans":[{"scope":{"name":"seatunnel.stain_trace"},"spans":[{"traceId":"000000000000000000000000000000c8","spanId":"00000000000000c8","parentSpanId":"","name":"seatunnel.record","kind":1,"startTimeUnixNano":"1708000000000000000","endTimeUnixNano":"1708000001000000000","attributes":[{"key":"seatunnel.table_id","value":{"stringValue":"table1"}},{"key":"seatunnel.sink_task_id","value":{"intValue":"2"}}],"events":[{"name":"SOURCE_EMIT","timeUnixNano":"1708000000000000000","attributes":[{"key":"seatunnel.stage_code","value":{"intValue":"1"}},{"key":"seatunnel.task_id","value":{"intValue":"1"}}]},{"name":"SINK_WRITE_DONE","timeUnixNano":"1708000001000000000","attributes":[{"key":"seatunnel.stage_code","value":{"intValue":"6"}},{"key":"seatunnel.task_id","value":{"intValue":"2"}}]}],"status":{"code":1}}]}]}]}
```

各字段说明：
- `resourceSpans[].resource.attributes`：作业元数据（`service.name`、`seatunnel.job_id`）
- `scopeSpans[].scope.name`：固定为 `"seatunnel.stain_trace"`
- `spans[]`：每条采样记录对应一个 span
  - `traceId` / `spanId`：128-bit / 64-bit 十六进制（由内部 64-bit id 零填充）
  - `startTimeUnixNano` / `endTimeUnixNano`：首尾阶段时间戳（纳秒，字符串）
  - `events[]`：每个阶段一个事件
    - `name`：阶段名称（如 `SOURCE_EMIT`、`QUEUE_IN`、`SINK_WRITE_DONE`）
    - `timeUnixNano`：阶段时间戳（纳秒）
    - `attributes`：`seatunnel.stage_code`（int）、`seatunnel.task_id`（int）

## 性能影响

| 场景 | 采样率 | 吞吐量 | 预期开销 |
|------|--------|--------|---------|
| 生产环境 | 1/100000 | 1M records/s | < 2% |
| 测试环境 | 1/1000 | 100K records/s | < 5% |
| 开发环境 | 1/100 | 10K records/s | < 10% |

## 分析工具

独立的分析工具生成 HTML 报告，包含：

- 端到端延迟分析
- 各阶段耗时统计
- 性能瓶颈识别
- 时间线可视化

## 故障排查

### 没有生成追踪文件

1. 检查引擎级和任务级开关是否都已启用
2. 验证 `stain-trace-file-base-path` 目录权限
3. 查看日志中的 "StainTrace" 或 "TraceFileWriter" 消息

### 空的追踪文件

当追踪写入器（`TraceFileWriter`）被初始化后、尚未写入任何事件前作业结束，会产生空文件。可以安全删除。

### 只有部分阶段数据

这种情况发生在 Transform 将 1 条记录分裂为 N 条记录时。默认情况下，只有第一条输出继承追踪负载。设置 `stain-trace-propagate-to-all-splits: true` 可追踪所有分裂。

### 看不到扩展阶段事件（101+、201+）

当前**只有 6 个基础阶段已落点**。101 系列、201 系列、网络传输（217-220）、流控（226-227）等扩展阶段仅在枚举中定义，尚未有生产代码调用点，不会出现在追踪文件中。

## 相关文档

- [快速开始指南](./stain-trace-quickstart.md)
- [事件监听器](../event-listener.md)
- [遥测](telemetry.md)
