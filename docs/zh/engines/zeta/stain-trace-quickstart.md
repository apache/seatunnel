# StainTrace - 快速开始指南

## 概述

StainTrace 是 SeaTunnel 的**数据血缘与端到端性能追踪系统**，用于追踪引擎内部的完整数据流。

### 核心特性

- **框架级实现**：对所有 Connector 开箱即用，无需修改 Connector 代码
- **6 个基础阶段**：S0 → Q+ → Q- → T+ → T- → W!（完整端到端链路，全部已落点）
- **扩展细粒度阶段**：定义了 40+ 个阶段码，供未来落点使用（见下方阶段表）
- **本地文件存储**：零依赖，JSON Lines 格式，轻量级
- **任务级控制**：引擎级 + 任务级双开关，灵活控制采样
- **离线分析工具**：独立分析器生成 HTML 报告
- **OpenTelemetry 集成**：原生支持 OTel Span JSON 格式
- **性能优化**：采样率控制 + 批量处理 + 限流，开销 < 2%

### 6 个基础追踪阶段

1. **S0**（SOURCE_EMIT）：Source 发出数据
2. **Q+**（QUEUE_IN）：进入队列
3. **Q-**（QUEUE_OUT）：离开队列
4. **T+**（TRANSFORM_IN）：Transform 接收数据
5. **T-**（TRANSFORM_OUT）：Transform 输出数据
6. **W!**（SINK_WRITE_DONE）：Sink 写入完成

### 阶段详情

#### 基础阶段（1-6）
| 阶段 | 编码 | 说明 | 落点位置 |
|------|------|------|---------|
| SOURCE_EMIT | 1 | Source 发出数据 | SeaTunnelSourceCollector.collect() |
| QUEUE_IN | 2 | 进入队列（入队前，捕捉背压） | IntermediateQueue.received() |
| QUEUE_OUT | 3 | 离开队列（出队后） | IntermediateQueue.collect() |
| TRANSFORM_IN | 4 | Transform 接收数据 | TransformFlowLifeCycle.received() |
| TRANSFORM_OUT | 5 | Transform 输出数据 | TransformFlowLifeCycle 输出前 |
| SINK_WRITE_DONE | 6 | Sink 写入完成 | SinkFlowLifeCycle.writer.write() 之后 |

#### 关键性能阶段（101-110）
> ⚠️ **规划中 — 尚未落点。** 这些阶段码已在 `StainTraceStage` 中定义，但目前没有生产代码调用点。在落点实现之前，它们不会出现在追踪文件中。

| 阶段 | 编码 | 说明 | 用途 |
|------|------|------|------|
| SOURCE_READ_END | 101 | Source 读取完成 | Source 读取性能 |
| QUEUE_OFFER_START | 102 | 队列入队开始 | 背压检测 |
| TRANSFORM_EXECUTE_START | 104 | Transform 执行开始 | Transform 性能分析 |
| TRANSFORM_EXECUTE_END | 105 | Transform 执行结束 | Transform 性能分析 |
| SINK_BATCH_AGGREGATE_END | 106 | Sink 批量聚合完成 | Sink 批处理性能 |
| SINK_FORMAT_END | 107 | Sink 格式化完成 | 数据格式化性能 |
| SINK_WRITE_START | 108 | Sink I/O 写入开始 | I/O 性能分析 |
| SINK_WRITE_END | 109 | Sink I/O 写入结束 | I/O 性能分析 |
| SINK_COMMIT_END | 110 | Sink 提交完成 | 事务提交性能 |

#### 扩展细粒度阶段（201-220）
> ⚠️ **规划中 — 尚未落点。**

| 阶段 | 编码 | 说明 | 用途 |
|------|------|------|------|
| SOURCE_READ_START | 201 | Source 读取开始 | Source 性能 |
| SOURCE_SERIALIZE_START/END | 202-203 | Source 序列化 | 序列化性能 |
| TRANSFORM_PARSE_START/END | 205-206 | Transform 解析 | 解析性能 |
| TRANSFORM_BUILD_START/END | 207-208 | Transform 结果构建 | 构建性能 |
| SINK_RECEIVE | 209 | Sink 接收数据 | 数据流追踪 |
| SINK_BATCH_AGGREGATE_START | 210 | Sink 批量聚合开始 | 批处理性能 |
| SINK_FORMAT_START | 211 | Sink 格式化开始 | 格式化性能 |
| SINK_COMMIT_START | 212 | Sink 提交开始 | 提交性能 |
| CHECKPOINT_SNAPSHOT_START/END | 213-214 | Checkpoint 快照 | Checkpoint 性能 |
| CHECKPOINT_BARRIER_EMIT/RECEIVE | 215-216 | Checkpoint Barrier | Barrier 传播 |

#### 网络传输阶段（217-220，仅多节点集群）
> ⚠️ **规划中 — 尚未落点。** 需要 hook 进 Hazelcast 序列化层以捕捉跨节点传输。

| 阶段 | 编码 | 说明 | 出现条件 |
|------|------|------|---------|
| RECORD_SERIALIZE_START | 217 | 数据序列化开始 | 多节点集群，跨节点数据传输 |
| RECORD_SERIALIZE_END | 218 | 数据序列化结束 | 同上 |
| RECORD_DESERIALIZE_START | 219 | 数据反序列化开始 | 同上 |
| RECORD_DESERIALIZE_END | 220 | 数据反序列化结束 | 同上 |

#### 流控审计阶段（226-227）
> ⚠️ **规划中 — 尚未落点。**

| 阶段 | 编码 | 说明 | 用途 |
|------|------|------|------|
| FLOW_CONTROL_AUDIT_START | 226 | 流控审计开始 | 背压检测 |
| FLOW_CONTROL_AUDIT_END | 227 | 流控审计结束 | 背压检测 |

> **⚠️ 重要说明**：
> - **单节点执行**：数据通过内存队列传输，序列化阶段（217-220）**不会出现**
> - **多节点集群**：当数据需要跨节点网络传输时，序列化阶段**才会出现**
> - 序列化耗时可通过 `gap_ms` 计算：`RECORD_SERIALIZE_END - RECORD_SERIALIZE_START`

### 本地文件存储

StainTrace 使用本地文件存储追踪数据：

- **零依赖**：无需数据库或外部服务
- **轻量级**：JSON Lines 格式，人类可读
- **离线分析**：独立分析工具生成 HTML 报告
- **存储路径**：由 `stain-trace-file-base-path` 显式配置，默认无值。例如：
  `/tmp/seatunnel/traces/traces/{job_id}/{yyyy-MM-dd}/`

---

## 快速开始

### 第一步：配置引擎

编辑 `seatunnel.yaml`：

```yaml
seatunnel:
  engine:
    # 启用 StainTrace（系统级主开关）
    stain-trace-enabled: true

    # 采样间隔：每 100 条记录采样 1 条（开发环境）
    # 生产环境推荐：100000-1000000
    stain-trace-sample-interval: 100

    # 本地文件存储根目录。该项必须显式配置。
    stain-trace-file-base-path: /tmp/seatunnel/traces

    # 每个文件最大事件数（达到后创建新文件）
    stain-trace-file-max-events-per-file: 10000

    # 每个文件最大大小（MB），达到后创建新文件
    stain-trace-file-max-size-mb: 10

    # 刷盘间隔（秒），批量写入间隔
    stain-trace-file-flush-interval-seconds: 10
```

### 第二步：启用任务级开关

在作业配置文件（`job.conf`）中启用：

```hocon
env {
  stain_trace {
    enabled = true
  }

  # 其他环境配置...
  parallelism = 2
  job.mode = "BATCH"
}

source {
  # ... 你的 Source 配置
}

transform {
  # ... 你的 Transform 配置
}

sink {
  # ... 你的 Sink 配置
}
```

### 第三步：运行作业

运行 SeaTunnel 作业后，追踪数据会保存到已配置的本地目录。

### 第四步：查看追踪文件

```bash
# 查看生成的文件列表
ls -lh /tmp/seatunnel/traces/traces/{job_id}/{yyyy-MM-dd}/

# 查看文件内容（JSON Lines 格式）
cat /tmp/seatunnel/traces/traces/{job_id}/{yyyy-MM-dd}/trace-*.jsonl
```

### 第五步：使用分析工具生成 HTML 报告

分析器是独立工具。将 `analyze-traces.sh` 和
`seatunnel-trace-analyzer-*-jar-with-dependencies.jar` 放在同一目录，然后运行：

```bash
# 分析追踪文件并生成 HTML 报告
./analyze-traces.sh /tmp/seatunnel/traces report.html

# 用浏览器打开报告
open report.html     # macOS
xdg-open report.html # Linux
```

**从源码构建 JAR**（仅开发环境）：
```bash
# 从项目根目录执行：
mvn clean package -pl seatunnel-trace/seatunnel-trace-analyzer -am
# JAR 位于：seatunnel-trace/seatunnel-trace-analyzer/target/seatunnel-trace-analyzer-*-jar-with-dependencies.jar
```

**完成！** 无需额外服务，追踪数据全部存储在本地文件中。

---

## 文件存储格式

### 目录结构

```
/tmp/seatunnel/traces/            ← stain-trace-file-base-path
└── traces/
    └── {job_id}/
        └── {yyyy-MM-dd}/
            ├── traces-14-30-00-a1b2c3d4.jsonl
            ├── traces-14-30-10-e5f6g7h8.jsonl
            └── ...
```

### 文件格式

每个文件为 JSON Lines 格式（每行一个 JSON 对象）。
每行是一个 **OTLP `ExportTraceServiceRequest`**，每个采样行对应一个 Span：

```json
{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"seatunnel"}},{"key":"seatunnel.job_id","value":{"stringValue":"123456"}}]},"scopeSpans":[{"scope":{"name":"seatunnel.stain_trace"},"spans":[{"traceId":"0000000000000000000000000000007b","spanId":"000000000000007b","parentSpanId":"","name":"seatunnel.record","kind":1,"startTimeUnixNano":"1708000000000000000","endTimeUnixNano":"1708000001000000000","attributes":[{"key":"seatunnel.table_id","value":{"stringValue":"table1"}},{"key":"seatunnel.sink_task_id","value":{"intValue":"123"}}],"events":[{"name":"SOURCE_EMIT","timeUnixNano":"1708000000000000000","attributes":[{"key":"seatunnel.stage_code","value":{"intValue":"1"}},{"key":"seatunnel.task_id","value":{"intValue":"1"}}]},{"name":"SINK_WRITE_DONE","timeUnixNano":"1708000001000000000","attributes":[{"key":"seatunnel.stage_code","value":{"intValue":"6"}},{"key":"seatunnel.task_id","value":{"intValue":"2"}}]}],"status":{"code":1}}]}]}]}
```

每行包含：
- `resourceSpans[].resource.attributes`：作业元数据（`service.name`、`seatunnel.job_id`）
- `resourceSpans[].scopeSpans[].scope.name`：`"seatunnel.stain_trace"`
- `spans[]`：每个采样行对应一个 Span
  - `traceId` / `spanId`：128 位 / 64 位十六进制（从内部 64 位 id 零填充扩展）
  - `startTimeUnixNano` / `endTimeUnixNano`：首尾阶段时间戳（纳秒，字符串）
  - `attributes`：行元数据（`seatunnel.table_id`、`seatunnel.sink_task_id`）
  - `events[]`：每个流水线阶段对应一个事件
    - `name`：阶段名称（如 `SOURCE_EMIT`、`QUEUE_IN`、`SINK_WRITE_DONE`）
    - `timeUnixNano`：阶段时间戳（纳秒，字符串）
    - `attributes`：`seatunnel.stage_code`（整数）、`seatunnel.task_id`（整数）

---

## 配置参考

### 引擎级配置（seatunnel.yaml）

```yaml
seatunnel:
  engine:
    # ==================== 基础配置 ====================
    # 启用 StainTrace（系统级主开关）
    stain-trace-enabled: true

    # 采样间隔：每 N 条记录采样 1 条（默认：100000）
    # 开发环境推荐：100-1000
    # 生产环境推荐：100000-1000000
    stain-trace-sample-interval: 100000

    # 每个 Worker 每秒最大 Trace 数（默认：50）
    # 控制追踪量，防止事件风暴
    stain-trace-max-traces-per-second-per-worker: 50

    # 每条 Trace 最大阶段条目数（默认：32）
    # 32 覆盖 99% 的流水线，避免 payload 膨胀
    stain-trace-max-entries-per-trace: 32

    # ==================== 高级配置 ====================
    # 是否将 payload 传播到所有分裂输出（默认：false）
    # false：1-to-N 场景只有第一条输出继承 payload
    # true：所有分裂输出均继承 payload（增加追踪数量）
    stain-trace-propagate-to-all-splits: false

    # ==================== 本地文件存储配置 ====================
    # 文件存储根目录。建议与 checkpoint.storage.plugin-config.namespace
    # 使用相同的存储根路径。示例：
    #   checkpoint namespace: /data/seatunnel/checkpoint_snapshot/
    #   trace base path:      /data/seatunnel/traces
    stain-trace-file-base-path: /tmp/seatunnel/traces

    # 每个文件最大事件数（默认：10000）
    # 达到后创建新文件
    stain-trace-file-max-events-per-file: 10000

    # 每个文件最大大小（MB）（默认：10）
    # 达到后创建新文件
    stain-trace-file-max-size-mb: 10

    # 刷盘间隔（秒）（默认：10）
    # 批量写入间隔，平衡性能与数据完整性
    stain-trace-file-flush-interval-seconds: 10
```

### 任务级配置（job.conf）

通过作业配置中的 `env` 块控制追踪启用状态：

```hocon
env {
  # 任务级 StainTrace 开关
  stain_trace {
    # 为该任务启用追踪（默认：false）
    # 注意：引擎级 stain-trace-enabled 也必须为 true
    enabled = true

    # 任务级采样间隔（可选，覆盖引擎级配置）
    # sample_interval = 1000
  }

  # 其他环境配置...
  parallelism = 2
  job.mode = "BATCH"
}
```

### 配置参数表

| 配置项 | 类型 | 默认值 | 说明 |
|-------|------|-------|------|
| **引擎级（seatunnel.yaml）** ||||
| `stain-trace-enabled` | Boolean | false | 引擎级主开关，必须为 true 才能启用 |
| `stain-trace-sample-interval` | Integer | 100000 | 采样间隔：每 N 条记录采样 1 条 |
| `stain-trace-max-traces-per-second-per-worker` | Integer | 50 | 每个 Worker 每秒最大 Trace 数 |
| `stain-trace-max-entries-per-trace` | Integer | 32 | 每条 Trace 最大阶段条目数 |
| `stain-trace-propagate-to-all-splits` | Boolean | false | 是否传播到所有分裂输出 |
| `stain-trace-file-base-path` | String | 无 | 本地文件存储根目录。未显式配置该路径时，本地文件输出保持关闭 |
| `stain-trace-file-max-events-per-file` | Integer | 10000 | 每个文件最大事件数 |
| `stain-trace-file-max-size-mb` | Integer | 10 | 每个文件最大大小（MB） |
| `stain-trace-file-flush-interval-seconds` | Integer | 10 | 刷盘间隔（秒） |
| **任务级（job.conf env 块）** ||||
| `stain_trace.enabled` | Boolean | false | 任务级开关，需要引擎级也已启用 |
| `stain_trace.sample_interval` | Integer | 继承引擎级 | 任务级采样间隔（可选） |

### 生效条件

StainTrace 最终生效条件：

```
effectiveEnabled = engineConfig.stainTraceEnabled && jobEnv.stainTrace.enabled
```

即：**引擎级和任务级开关都必须启用**，追踪才会生效。

---

## 验证

运行作业后，检查以下内容：

### 1. 检查文件是否生成

```bash
# 查看文件列表
ls -lh /tmp/seatunnel/traces/traces/{job_id}/{yyyy-MM-dd}/

# 查看文件内容
head -n 5 /tmp/seatunnel/traces/traces/{job_id}/{yyyy-MM-dd}/trace-*.jsonl
```

### 2. 验证数据完整性

你应该看到：
- 每行是完整的 OTLP JSON 对象（`resourceSpans` → `scopeSpans` → `spans`）
- 每个 Span 的 `events[]` 包含 **6 个基础阶段**的条目（SOURCE_EMIT、QUEUE_IN、QUEUE_OUT、TRANSFORM_IN、TRANSFORM_OUT、SINK_WRITE_DONE）——这些是唯一保证出现的阶段；扩展阶段（101+）需要额外的落点实现
- 阶段时间戳单调递增（S0 < Q+ < Q- < T+ < T- < W!）

### 3. 使用分析工具

```bash
./analyze-traces.sh /tmp/seatunnel/traces report.html
open report.html
```

分析工具生成的 HTML 报告包含：
- 端到端延迟分析
- 各阶段耗时统计
- 性能瓶颈识别
- 时间线可视化

### 4. 示例作业特征

默认示例作业 `stain_trace_fake_sql_union_to_console.conf`：
- **FakeSource**：生成 10 条记录
- **Sql Transform**：使用 LATERAL VIEW EXPLODE，1 条输入 → 2 条输出
- **Console Sink**：输出 20 条记录
- **采样率**：sample-rate=1（全量采样）
- **预期 Trace 数**：10 条（只有第一条分裂继承 payload）

---

## 故障排查

### 问题 1：找不到追踪文件

**排查步骤**：

1. 确认文件存储路径：
```bash
# 默认路径
ls -lh /tmp/seatunnel/traces/traces/

# 检查 job_id 目录是否存在
ls -lh /tmp/seatunnel/traces/traces/{job_id}/
```

2. 检查权限：
```bash
# 确保目录可写
ls -ld /tmp/seatunnel/traces/
```

3. 检查配置：
```yaml
stain-trace-file-base-path: /tmp/seatunnel/traces  # 确认路径正确
```

4. 查看作业日志，搜索 "StainTrace" 或 "TraceFileWriter"

### 问题 2：没有追踪数据

**排查步骤**：

1. 确认引擎级开关已启用：
```bash
grep -A 5 "stain-trace" config/seatunnel.yaml
```

2. 确认任务级开关已启用：
```bash
grep -A 3 "stain_trace" examples/your-job.conf
```

3. 验证配置：
```hocon
env {
  stain_trace {
    enabled = true  # 必须显式启用
  }
}
```

**记住**：**引擎级和任务级开关都必须启用**，追踪才会生效！

### 问题 3：只有部分阶段数据

**原因**：在 Transform 的 1-to-N 场景中，只有第一条输出继承 payload

**验证**：检查 `stain-trace-propagate-to-all-splits` 配置
```yaml
stain-trace-propagate-to-all-splits: false  # 只有第一条继承（默认）
stain-trace-propagate-to-all-splits: true   # 所有分裂均继承
```

### 问题 4：看不到序列化事件（阶段 217-220）

**现象**：Stage 明细中没有 RECORD_SERIALIZE_START/END 或 RECORD_DESERIALIZE_START/END

**原因**：
- **单节点执行**：数据通过内存队列传输，无需序列化，这些阶段不会出现
- 只有在多节点集群执行时，数据需要跨节点网络传输，才会触发序列化

**解决方案**：
- 要测试序列化性能，需要搭建多节点 SeaTunnel 集群
- 单节点测试可忽略序列化事件，关注其他阶段

### 问题 5：文件太大或太多

**调整配置**：

```yaml
# 增大采样间隔以减少追踪数量
stain-trace-sample-interval: 1000000  # 每 100 万条记录采样 1 条

# 增大文件大小限制
stain-trace-file-max-size-mb: 50

# 增大每文件事件数
stain-trace-file-max-events-per-file: 50000
```

### 问题 6：文件权限问题

**报错**：`Permission denied` 或 `Cannot create directory`

**解决方案**：
```bash
# 创建目录并设置权限
sudo mkdir -p /tmp/seatunnel/traces
sudo chmod 777 /tmp/seatunnel/traces

# 或使用用户目录
stain-trace-file-base-path: ~/seatunnel/traces
```

---

## 高级用法

### 自定义作业配置

创建自己的作业配置文件，参考：
```bash
seatunnel-examples/seatunnel-engine-examples/src/main/resources/examples/stain_trace_fake_sql_union_to_console.conf
```

运行时指定：
```java
public static void main(String[] args) {
    String configurePath = "/path/to/your/job.conf";
    // ... 其余代码
}
```

### 性能调优

#### 开发环境配置（高采样率，便于调试）
```yaml
stain-trace-enabled: true
stain-trace-sample-interval: 100  # 每 100 条采样 1 条
stain-trace-max-traces-per-second-per-worker: 1000
stain-trace-max-entries-per-trace: 64
stain-trace-file-base-path: /tmp/seatunnel/traces
stain-trace-file-flush-interval-seconds: 5  # 更频繁刷盘
```

#### 生产环境配置（低开销，大规模）

> **路径一致性**：`stain-trace-file-base-path` 建议与 `checkpoint.storage.plugin-config.namespace`
> 使用**相同的存储根路径**。例如，若 Checkpoint namespace 为 `/data/seatunnel/checkpoint_snapshot/`，
> 则将 Trace 路径配置为 `/data/seatunnel/traces`。使用 HDFS 备份的 Checkpoint 时，
> 两者都应指向同一 HDFS 路径前缀，以确保存储和保留策略统一生效。

```yaml
seatunnel:
  engine:
    stain-trace-enabled: true
    stain-trace-sample-interval: 100000  # 每 10 万条采样 1 条
    stain-trace-max-traces-per-second-per-worker: 50
    stain-trace-max-entries-per-trace: 32
    # 与 checkpoint.storage.plugin-config.namespace 保持相同存储根路径
    stain-trace-file-base-path: /data/seatunnel/traces  # 对应 namespace: /data/seatunnel/checkpoint_snapshot/
    stain-trace-file-max-events-per-file: 50000  # 更大的文件
    stain-trace-file-max-size-mb: 50
    stain-trace-file-flush-interval-seconds: 30  # 降低刷盘频率
    checkpoint:
      storage:
        type: localfile           # 或 hdfs（HDFS 集群）
        plugin-config:
          namespace: /data/seatunnel/checkpoint_snapshot/
```

#### 性能影响

优化后 StainTrace 的性能影响：

| 指标 | 数值 |
|------|------|
| 1/100000 采样率下的 CPU 开销 | **< 2%** |
| 1/1000 采样率下的 CPU 开销 | < 5% |
| 每条记录的 Trace payload 大小 | ~1KB（32 个阶段） |
| Arrays.copyOf 调用次数减少 | **60% ~ 70%** |
| System.currentTimeMillis 调用次数减少 | **50%** |

### 按作业控制采样率

不同作业可以使用不同的采样率：

```hocon
# 高吞吐量作业：低采样率
env {
  stain_trace {
    enabled = true
    sample_interval = 1000000  # 每 100 万条采样 1 条
  }
}
```

```hocon
# 调试作业：高采样率
env {
  stain_trace {
    enabled = true
    sample_interval = 10  # 每 10 条采样 1 条
  }
}
```

### 定期清理旧文件

```bash
# 删除 7 天前的追踪文件
find /tmp/seatunnel/traces/traces -type f -name "*.jsonl" -mtime +7 -delete

# 或使用 crontab 定时清理
# 每天凌晨 2 点清理 7 天前的文件
0 2 * * * find /tmp/seatunnel/traces/traces -type f -name "*.jsonl" -mtime +7 -delete
```

---

## 性能优化成果

StainTrace 已完成优化，确保生产就绪：

### 核心优化（已完成 ✅）

1. **批量追加 API**
   - 一次追加多个阶段，减少数组拷贝
   - Arrays.copyOf 调用次数减少 **60-70%**

2. **时间戳优化**
   - 批量操作共享时间戳
   - System.currentTimeMillis 调用次数减少 **50%**

3. **本地文件存储**
   - 零依赖，无需数据库
   - JSON Lines / OTLP JSON 格式，人类可读
   - 独立分析工具生成 HTML 报告

### 规划中的落点（尚未实现）

4. **网络序列化追踪** *(规划中)*
   - RECORD_SERIALIZE_START/END（217-220）
   - 将精确识别跨节点网络传输瓶颈
   - 需要 hook 进 Hazelcast 序列化层

5. **流控审计追踪** *(规划中)*
   - FLOW_CONTROL_AUDIT_START/END（226-227）
   - 将识别背压问题

6. **按 Transform 执行追踪** *(规划中)*
   - TRANSFORM_EXECUTE_START/END（104-105）
   - 将识别 Transform 链中的具体瓶颈

---

## 规划功能详情

### 按 Transform 执行追踪（TRANSFORM_EXECUTE_START / TRANSFORM_EXECUTE_END）

**阶段码**：104 / 105 — **尚未落点**

#### 问题背景

当作业包含 3 个及以上 Transform 插件链时，6 个基础阶段只能观察到**整条链路的总延迟**
（`TRANSFORM_IN → TRANSFORM_OUT`）。如果链中某个 Transform 很慢，单靠追踪无法定位是哪一个。

例如，对于链路：`FieldMapper → SqlTransform → CopyField`，当前追踪展示为：

```
T+（TRANSFORM_IN）→  T-（TRANSFORM_OUT）
      ↑                      ↑
   链路开始               链路结束
   （总耗时 = 120 ms，但哪一步花了 100 ms？）
```

#### 落点后的效果

链中每个 Transform 将发出自己的 START/END 对：

```
T+ → TRANSFORM_EXECUTE_START[FieldMapper] → TRANSFORM_EXECUTE_END[FieldMapper]
   → TRANSFORM_EXECUTE_START[SqlTransform] → TRANSFORM_EXECUTE_END[SqlTransform]
   → TRANSFORM_EXECUTE_START[CopyField]    → TRANSFORM_EXECUTE_END[CopyField]
   → T-
```

这使按 Transform 定位瓶颈变得精确：

| 阶段 | 时间（ms） | 耗时 |
|------|-----------|------|
| TRANSFORM_IN | 0 | — |
| TRANSFORM_EXECUTE_START（FieldMapper） | 1 | — |
| TRANSFORM_EXECUTE_END（FieldMapper） | 3 | **2 ms** |
| TRANSFORM_EXECUTE_START（SqlTransform） | 3 | — |
| TRANSFORM_EXECUTE_END（SqlTransform） | 101 | **98 ms** ← 瓶颈 |
| TRANSFORM_EXECUTE_START（CopyField） | 101 | — |
| TRANSFORM_EXECUTE_END（CopyField） | 103 | **2 ms** |
| TRANSFORM_OUT | 104 | — |

#### 条目预算

每个 Transform 消耗 **2 个 payload 槽**（START + END）。5 个 Transform 的链路在 6 个基础阶段之上
额外增加 10 个条目。默认的 `stain-trace-max-entries-per-trace: 32` 可轻松容纳最多 13 个
Transform 的链路而不截断。

如果你的 Transform 链路非常长且在日志中看到截断警告，请增大限制：
```yaml
stain-trace-max-entries-per-trace: 64
```

#### 当前临时方案

在该功能落点之前，可以通过以下方式间接估算各 Transform 耗时：
1. 每次只启用一个 Transform 运行作业，比较 `T+ → T-` 的间隔
2. 通过 SeaTunnel 现有指标系统查看 Transform 相关指标

---

### 网络序列化追踪（RECORD_SERIALIZE_START/END · RECORD_DESERIALIZE_START/END）

**阶段码**：217 / 218 / 219 / 220 — **尚未落点**

> ⚠️ 这些阶段**仅与多节点集群部署相关**。在单节点模式下，数据通过内存队列传输，
> 不会发生序列化——即使落点实现后，这些阶段也不会出现。

#### 问题背景

在多节点 SeaTunnel 集群中，记录通过 Hazelcast 的网络队列跨越节点边界。
每次跨越都会产生**序列化开销**（发送节点）和**反序列化开销**（接收节点）。
目前这部分开销对 StainTrace 完全不可见；该间隔在 `QUEUE_IN` 和 `QUEUE_OUT` 之间
表现为死时间。

示例（当前追踪 —— 2 节点集群，Source 在节点 A，Sink 在节点 B）：

```
S0（节点 A）→ Q+（节点 A）→  ??? 15 ms 间隔 ???  → Q-（节点 B）→ … → W!
                                   ↑
                          网络序列化开销隐藏于此
```

#### 落点后的效果

每次网络跳转将在跨节点传输前后发出 4 个事件：

```
Q+ → RECORD_SERIALIZE_START → RECORD_SERIALIZE_END
   → [网络传输] →
     RECORD_DESERIALIZE_START → RECORD_DESERIALIZE_END → Q-
```

这将原本不透明的网络间隔分解为：

| 阶段 | 节点 | 含义 |
|------|------|------|
| RECORD_SERIALIZE_START | 发送方 | Hazelcast 开始编码该行 |
| RECORD_SERIALIZE_END | 发送方 | 编码完成；字节交给 socket |
| RECORD_DESERIALIZE_START | 接收方 | Hazelcast 开始解码收到的字节 |
| RECORD_DESERIALIZE_END | 接收方 | 行完全重建；可以继续处理 |

这些事件的 `task_id` 属性将是保留常量（`-1`），标识网络序列化器而非流水线任务。

#### 条目预算

每次网络跳转增加 **4 个条目**（serialize-start/end + deserialize-start/end）。
在默认的 `stain-trace-max-entries-per-trace: 32` 下，6 个基础阶段已占用 6 个槽，
最多可追踪 **6 次跨节点跳转**而不截断。

#### 如何确认是否处于多节点集群

如果需要网络序列化阶段，请验证部署方式：

```bash
# 检查集群成员
curl http://localhost:5801/hazelcast/rest/cluster
# 如果 members > 1，则处于多节点集群
```

#### 当前临时方案

在该功能落点之前，可通过以下方式估算跨节点序列化开销：

```
serialization_overhead ≈ gap(Q-, Q+) - expected_queue_wait_time
```

对于负载轻且无背压的集群，`Q+ → Q-` 间隔中几乎全部是序列化 + 网络延迟。

---

### 预期结果

| 场景 | 采样率 | 吞吐量 | 预期开销 |
|------|-------|-------|---------|
| 生产环境 | 1/100000 | 100 万条/秒 | **< 2%** |
| 测试环境 | 1/1000 | 10 万条/秒 | < 5% |
| 开发环境 | 1/100 | 1 万条/秒 | < 10% |
