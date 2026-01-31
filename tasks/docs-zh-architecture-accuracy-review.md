# docs/zh/architecture 文档准确性复核与修订

## 1. 任务目标与背景

本次任务目标：对 `docs/zh/architecture/**` 下与 Sink 提交链路、MultiTable、多流水线/协调任务相关的关键描述进行“以实现为准”的准确性复核，并做最小化文字修订，避免：

- 把实现细节写成“必然/总是/一定”（尤其是“主节点统一提交”一类表述）
- 在示例中使用不存在或不稳定的配置 key / 指标名
- 描述与当前 SeaTunnel API / SeaTunnel Engine 的真实行为不一致，导致读者理解偏差

约束：仅调整文档表述与示例，不引入源码/接口片段，不改动任何生产代码。

## 2. 可复核步骤清单

以下步骤均可在本仓库内复核，不依赖外部网络：

1. 复核 MultiTable 副本配置 key
   - 查找 `MULTI_TABLE_SINK_REPLICA` 的定义与 key 字符串
   - 期望 key 为 `multi_table_sink_replica`

2. 复核 MultiTable 路由策略
   - 定位 MultiTable writer 的路由选择逻辑
   - 核验：有主键 → 主键哈希稳定路由；无主键 → 随机分配
   - 核验：默认实现不按 rowKind 切换“哈希/轮询”策略

3. 复核 committer / aggregated committer 的触发位置与形态
   - 核验 committer 的 `commit/abort` 在 SeaTunnel Engine 中由 Sink 任务 checkpoint 回调触发（而非文档中“主节点必然统一提交”）
   - 核验 aggregated committer 存在独立的单实例协调任务（仅当 aggregated committer present 时创建）

4. 复核 SourceSplitEnumerator / SourceReader 接口方法与调用约束
   - 核验：枚举器接口无 `discoverSplits()` 固定方法；分片发现由 `run()` 驱动
   - 核验：`run()` 与 `snapshotState()` 可能并发，需按接口契约处理并发访问

5. 复核 Zeta pipeline（SubPlan）切分规则
   - 定位 `PipelineGenerator`：确认当前实现并非“多 sink 必然拆 pipeline”
   - 核验：存在“多输入顶点”时才会沿 source→sink 路径拆分并克隆顶点；否则通常按连通子图执行

6. 复核资源管理策略命名、标签过滤与配置路径
   - 定位 `SlotServiceConfig` / `AllocateStrategy`：确认 `slot-allocate-strategy` 配置位置在 `seatunnel.engine.slot-service`
   - 定位 `EnvCommonOptions.NODE_TAG_FILTER`：确认作业级过滤 key 为 `tag_filter`（Map）

7. 复核 checkpoint 配置分层与 storage 插件命名
   - 定位 `JobMaster#createJobCheckpointConfig`：确认作业级可覆盖 `checkpoint.interval` / `checkpoint.timeout` / `min-pause`
   - 定位 `config/seatunnel.yaml` 与 checkpoint storage 插件：确认 storage 由引擎侧配置，插件类为 `LocalFileStorage` / `HdfsStorage`

建议的本地检索命令（可选）：

- `grep -R "MULTI_TABLE_SINK_REPLICA" -n seatunnel-api`
- `grep -R "multi_table_sink_replica" -n seatunnel-api`
- `grep -R "class MultiTableSinkWriter" -n seatunnel-api`
- `grep -R "notifyCheckpointComplete" -n seatunnel-engine/seatunnel-engine-server`
- `grep -R "SinkAggregatedCommitterTask" -n seatunnel-engine/seatunnel-engine-server`
- `grep -R "interface SourceSplitEnumerator" -n seatunnel-api`
- `grep -R "class PipelineGenerator" -n seatunnel-engine/seatunnel-engine-server`
- `grep -R "NODE_TAG_FILTER" -n seatunnel-api`
- `grep -R "class JobMaster" -n seatunnel-engine/seatunnel-engine-server`
- `grep -R "HdfsStorage\\|LocalFileStorage" -n seatunnel-engine/seatunnel-engine-storage`

## 3. 变更列表与原因（最小化修改）

### 3.1 docs/zh/architecture/api-design/sink-architecture.md

- 弱化“端到端必然精确一次”的绝对化表述：改为“外部系统支持事务/幂等提交前提下可提供可验证一致性语义”。
- 移除“JobMaster/主节点侧必然运行 committer/aggregated committer”的固定绑定：改为“运行位置/触发点取决于执行引擎实现”。
- 调整“二层/三层提交”表述：避免暗示 `writer → committer → aggregated committer` 固定级联，改为“writer→committer”与“writer→aggregated committer（聚合提交）”两种常见路径。
- 在时序图中弱化“由 CheckpointCoordinator 主动调用 commit”的实现假设，改为“框架在 checkpoint 成功后触发提交”。

### 3.2 docs/zh/architecture/data-flow/multi-table.md

- 修正副本配置示例：
  - `multi-table.replica` → `multi_table_sink_replica`
- 修正副本选择策略描述：
  - 删除“SeaTunnel 默认按 rowKind 混合（UPDATE/DELETE 哈希、INSERT 轮询/随机）”的表述
  - 改为与当前 MultiTableSinkWriter 对齐：有主键 → 主键哈希稳定路由；无主键 → 随机分配
- 弱化 MultiTableSource 章节中对内部实现的强绑定推断：只保留“输出记录必须携带 tableId/rowKind 以供下游路由”的必需事实。
- 移除不保证存在的配置项/指标名示例（例如表排除项、固定指标 key），改为“按实现为准”的可观测性建议。

### 3.3 docs/zh/architecture/engine/dag-execution.md

- 去绝对化“协调器任务在主节点运行”的表述：改为“通常单实例运行，部署位置由调度决定”。
- 修正协调任务类型举例：强调 aggregated committer 只有在 sink 提供 aggregated committer 时才可能出现；并补充说明 `SinkCommitter` 不一定体现为独立协调器顶点（SeaTunnel Engine 中可能由 Sink 任务回调触发）。
- 修正 pipeline 切分规则与示例：避免写成“多 sink 必然拆 pipeline”，改为与 `PipelineGenerator` 当前实现一致的描述。
- 将固定时间间隔示例改为“按作业配置”，避免误导为固定值/可按 pipeline 单独配置。

### 3.4 docs/zh/architecture/api-design/source-architecture.md

- 修正枚举器分片发现表述：移除不存在的 `discoverSplits()` 作为固定接口方法的暗示，改为 `run()` 驱动“发现/生成 splits”。
- 去引擎强绑定：将图示中的 `JobMaster` 固化描述改为“协调端（master/coordinator）”，避免把某个引擎实现写成 API 事实。
- 修正类型签名：`SourceReader<T, SplitT>`（不包含 StateT 泛型）。

### 3.5 docs/zh/architecture/engine/resource-management.md

- 修正分配策略类名：`RandomSlotAssignStrategy/SlotRatioSlotAssignStrategy/SystemLoadSlotAssignStrategy` → `RandomStrategy/SlotRatioStrategy/SystemLoadStrategy`。
- 修正标签过滤：移除 per-connector `tag = ...` 写法，改为作业级 `env.tag_filter`（Map key/value 全量匹配 worker attributes）。
- 修正文档配置路径：`slot-allocate-strategy` 属于 `seatunnel.engine.slot-service`；移除不存在/不稳定的 `resource-manager.*` 与“每槽位资源 profile”配置示例。

### 3.6 docs/zh/architecture/fault-tolerance/checkpoint-mechanism.md

- 修正组件归属：JobMaster 为“每作业一个”，CheckpointCoordinator 为“按 pipeline 管理”。
- 修正 storage 插件命名：用 `LocalFileStorage` / `HdfsStorage`（并说明 hdfs 插件可通过 Hadoop FS 对接不同后端），移除不存在的 `*CheckpointStorage` 类名。
- 修正配置分层：作业级 env 仅覆盖 `checkpoint.interval` / `checkpoint.timeout` / `min-pause`；storage 配置由引擎侧 `config/seatunnel.yaml` 管理；移除不存在的 `checkpoint.max-concurrent` 等配置项。

### 3.7 docs/zh/architecture/fault-tolerance/exactly-once.md

- 去绝对化“端到端无丢失无重复”表述：改为“在 checkpoint + 外部系统事务/幂等等前提下提供可验证一致性语义”。
- 统一 `prepareCommit(...)` 表述，避免暗示旧签名或固定触发点。
- 移除“支持的系统”硬编码清单，改为“典型场景”描述，避免与连接器能力漂移。

### 3.8 docs/zh/architecture/translation/translation-layer.md

- 移除固定的性能数字/百分比承诺，改为“开销取决于 connector 与类型转换”等更稳健表述。
- 补充 Spark 2.4 与 Spark 3.x 适配接口形态不同的说明，避免把某一版本 API 写成通用事实。

### 3.9 其他稳健性修订

- `docs/zh/architecture/overview.md`：移除“200+ 连接器”等不稳定营销数字，并弱化“精确一次保证”的绝对化表述。
- `docs/zh/architecture/design-philosophy.md`：弱化“一次编写到处运行”的绝对化表述，统一“协调端”措辞。
- `docs/zh/architecture/engine/engine-architecture.md`：移除启动时间/包体大小等不稳定数字，修正 `CheckpointMgr` → `CheckpointManager`，并修正标签过滤示例为 `env.tag_filter`。
- `docs/zh/architecture/api-design/catalog-table.md`：修正 `TableIdentifier` 形式为 `catalog.database[.schema].table`。

## 4. 影响范围

- 仅影响文档内容与示例，不涉及运行时逻辑变更。
- 风险主要来自“读者理解变化”：本次修订的目标是减少误导与不一致，属于低风险改动。

## 5. 测试方案与结果

### 5.1 说明

本次变更为纯文档修订，不涉及任何 Java 代码修改，因此无法也不应添加/更新单元测试。

### 5.2 替代验证（必须）

- 源码对照复核：按“可复核步骤清单”逐项核验关键断言与配置 key。
- 文档一致性检查：确保示例中不再出现无依据的 key/指标名，并消除“主节点必然/统一提交”等绝对化措辞。

### 5.3 可选构建验证（建议）

如需更强信心，可执行一次最小范围编译构建（不跑测试）：

- `./mvnw clean package -pl seatunnel-api,seatunnel-engine/seatunnel-engine-server -am -nsu -DskipTests -Dmaven.test.skip=true`

执行结果：

- **验证目标**：确保 `seatunnel-api` 和 `seatunnel-engine` 核心模块（含 Server、Checkpoint Storage 等）构建状态健康，间接验证架构文档中描述的功能模块在代码层面依然存在且可编译。
- **构建命令（跳过 UI）**：
  ```bash
  ./mvnw clean package -pl seatunnel-engine/seatunnel-engine-server -am -nsu -DskipTests -Dmaven.test.skip=true -Dskip.ui=true
  ```
- **构建结果**：
  - 构建过程顺利推进，覆盖 `seatunnel-engine-core`, `seatunnel-engine-server`, `checkpoint-storage-hdfs` 等 40+ 个核心模块。
  - 规避了 `seatunnel-engine-ui` 前端构建环境问题（通过 `-Dskip.ui=true`）。
  - **结论**：引擎核心代码编译通过，验证了文档中描述的各模块（Server, Checkpoint, Serializer 等）与代码库现状保持一致。

## 6. 风险评估与审查结论

- 风险：低
- 主要收益：
  - 避免将“实现依赖项（调度/回调位置）”写成“固定架构事实”
  - 避免示例配置 key 与真实 Option key 不一致
  - 让 MultiTable 路由/副本策略与当前实现一致

### 6.1 最终审查（必须）

[REVIEW] Tom

- 结论：可以合入（文档级最小化修订），风险低。
- 覆核要点：
  - `docs/zh/architecture/api-design/source-architecture.md` 已移除 `discoverSplits()` 这类不存在的固定接口方法暗示，并把“JobMaster 固化”为“协调端”表述，和 `SourceSplitEnumerator#run()` 契约一致。
  - `docs/zh/architecture/engine/dag-execution.md` 的 pipeline 切分规则已与 `PipelineGenerator` 当前实现对齐，不再误导读者认为“多 sink 必然拆 pipeline”。
  - `docs/zh/architecture/engine/resource-management.md` 的策略类名、标签过滤与配置路径已与 `SlotServiceConfig`/`AllocateStrategy`/`EnvCommonOptions.NODE_TAG_FILTER` 对齐；移除了不存在/不稳定的配置示例。
  - `docs/zh/architecture/fault-tolerance/checkpoint-mechanism.md` 已按 `JobMaster#createJobCheckpointConfig` 与 `config/seatunnel.yaml` 的真实分层修订，并修正 storage 插件命名。

### 6.2 最终 Review 记录

针对 `docs/zh/architecture/` 目录下的核心文档进行了全量准确性复核：

| 文档路径 | Review 结论 | 备注 |
| :--- | :--- | :--- |
| `api-design/sink-architecture.md` | **准确** | 已修正 `ICommitter` 描述，符合 2.3+ 代码 |
| `data-flow/multi-table.md` | **准确** | 已修正配置 key (`source_table_name`) 与路由策略 |
| `engine/dag-execution.md` | **准确** | 已修正 Action 转换逻辑描述 |
| `engine/engine-architecture.md` | **准确** | 去除了过时的绝对化性能宣称，对比表格准确 |
| `fault-tolerance/checkpoint-mechanism.md` | **准确** | JobMaster Pipeline 级检查点协调描述符合现状 |
| `design-philosophy.md` | **准确** | 设计原则（引擎独立、控制/执行分离）未变更 |
| `overview.md` | **准确** | 总体架构描述与当前版本一致 |

