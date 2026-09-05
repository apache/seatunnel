# STIP：升级兼容性计划（第二阶段）

**STIP 编号**：待定
**状态**：草案
**作者**：zhang-arvin
**创建日期**：2026-08-20
**关联 Issue**：[#11356](https://github.com/apache/seatunnel/issues/11356)、[#11239](https://github.com/apache/seatunnel/issues/11239)
**关联 PR**：[#11301](https://github.com/apache/seatunnel/pull/11301)（第一阶段）

## 摘要

第一阶段（PR #11301）添加了一个定时触发和手动触发的跨版本恢复工作流，包含两个代表性场景。
本 STIP 定义了第二阶段：将该初始工作流转变为**可维护的升级兼容性计划**，具备文档化的兼容性契约、
扩展的场景覆盖范围以及清晰的运维策略。

## 动机

SeaTunnel 有多个有状态的升级路径，可能在不同版本间静默中断：

- 检查点/保存点恢复
- Source 枚举器状态恢复
- Sink 写入器状态恢复
- CDC 恢复/升级后继续读取
- 序列化的作业图和配置兼容性

PR #11301 提供了重要的第一步，但尚未回答发布级别的核心问题：
**在发布会影响持久化运行时状态的变更之前，我们实际验证了哪些升级保证？**

剩余差距包括：

1. 场景覆盖范围有意地很小（仅 2 个场景）
2. 并非所有高风险状态接口都被覆盖
3. 没有明确的策略规定何时必须在 PR 上运行升级检查
4. 失败输出未清晰标识哪个阶段中断
5. 社区缺少书面的兼容性边界文档

## 范围

### 范围内

- 定义必须在 `dev` 分支上保持通过的**最低支持升级场景**
- 扩展有状态 source、sink、CDC 和检查点/恢复路径的场景覆盖
- 定义触发策略：工作流何时运行（定时、手动、PR 门控）
- 使失败信息可操作：标识中断发生在状态创建、恢复、续跑还是恢复后验证阶段
- 记录工作流所断言的兼容性契约
- 中英文文档更新

### 范围外（第二阶段）

- 对每个连接器和每个历史版本的详尽覆盖
- 立即将每个 PR 变为完整的兼容性矩阵运行
- 声称比工作流实际验证更强的保证

## 设计

### 1. 支持的版本矩阵

升级兼容性工作流测试 **N-1 → dev** 恢复路径，其中 N-1 是工作流运行时最新的稳定 SeaTunnel 版本。

| 组件 | 版本策略 | 原因 |
|------|---------|------|
| 旧版本（保存点来源） | 最新稳定版本（如 2.3.13） | 代表最常见的用户升级路径 |
| 当前构建（恢复目标） | `dev` 分支 HEAD | 在发布前捕获回归 |
| 未来版本 | 添加 N-2 作为可选手动触发 | 优先级较低；N-1 覆盖大多数用户 |

**版本获取**：旧版本二进制文件从 Apache 镜像下载（`downloads.apache.org` / `archive.apache.org`）。
工作流参数 `old_version` 默认为最新的稳定版本，由维护者在每次发布后更新。

### 2. 场景选择策略

场景基于 **风险 × 覆盖** 选择：每个场景应测试一个历史上产生过回归的有状态接口。

#### 第二阶段最低场景集

| # | 场景 | 测试的状态接口 | 风险等级 |
|---|------|-------------|---------|
| 1 | `generic-fake-localfile`（已有） | Zeta 检查点，LocalFile sink 写入器状态 | 中 |
| 2 | `mysql-cdc-multitable-localfile`（已有） | CDC source 枚举器 + reader 状态，多表恢复 | 高 |
| 3 | `kafka-source-localfile` | Source split 枚举器状态，Kafka offset 恢复 | 中 |
| 4 | `jdbc-sink-postgres` | JDBC sink 写入器状态，精确一次语义 | 中 |
| 5 | `mysql-cdc-to-jdbc-sink` | 完整 CDC 管道：source 状态 + sink 写入器状态 | 高 |

#### 场景目录结构

每个场景位于 `tools/upgrade_compatibility/scenarios/<name>/` 下：

```
<name>/
├── seatunnel.yaml          # 引擎配置模板（使用 __CHECKPOINT_DIR__）
├── job.conf                # 流式作业模板（使用 __SINK_DIR__）
├── assert.conf             # 批量断言作业模板
├── plugin_config           # 旧版本所需的连接器构件
├── setup.sh                # 可选：外部服务设置（如 Docker）
├── teardown.sh             # 可选：外部服务清理
├── after_restore.sh        # 可选：恢复后插入标记数据的钩子
└── endless                 # 可选：流式作业标记（保存点后取消）
```

### 3. 高风险状态接口

以下代码区域被归类为**兼容性敏感**。对这些区域的变更应触发升级兼容性工作流：

| 接口 | 涉及模块 | 示例 |
|------|---------|------|
| 检查点序列化 | `seatunnel-api`、`seatunnel-engine` | `CheckpointState`、`StateSerializer` |
| Source 枚举器状态 | `seatunnel-api`、连接器 source 模块 | `SourceSplitEnumerator` 状态 |
| Sink 写入器状态 | `seatunnel-api`、连接器 sink 模块 | `SinkWriter` 状态、事务状态 |
| CDC offset 模型 | CDC 连接器模块 | `LsnOffset`、`ScnOffset`、`BinlogOffset` |
| 作业图/配置 | `seatunnel-api`、`seatunnel-core` | `JobConfig`、`Action`、`SeaTunnelConfig` |
| 序列化框架 | `seatunnel-api` | `Serializable` 契约变更 |

### 4. 触发/门控策略

工作流采用**渐进式门控**模型：

#### 阶段 2a：当前状态（已实现）

```
schedule:        每日 18:00 UTC（dev 分支）
workflow_dispatch: 手动触发，支持 old_version + scenario 输入
```

#### 阶段 2b：PR 选择性触发

添加基于**路径过滤**的 `pull_request` 触发器，针对兼容性敏感区域：

```yaml
on:
  pull_request:
    paths:
      - 'seatunnel-api/**'
      - 'seatunnel-engine/**'
      - 'seatunnel-core/**'
      - 'seatunnel-connectors-v2/connector-cdc/**'
      - 'seatunnel-connectors-v2/connector-kafka/**'
      - 'seatunnel-connectors-v2/connector-jdbc/**'
      - 'seatunnel-connectors-v2/connector-file/**'
      - 'seatunnel-connectors-v2/connector-fake/**'
      - 'seatunnel-translation/**'
```

此触发器最初为**非阻塞**（状态检查为建议性）。稳定运行 4 周后，可将状态检查提升为过滤路径的必选项。

#### 阶段 2c：发布门控（未来）

在每个候选版本发布前，手动运行完整场景矩阵，由发布经理审核结果。

### 5. 失败分类

失败按**阶段**分类，使输出可立即操作：

| 阶段 | 失败特征 | 责任人 |
|------|---------|-------|
| 状态创建 | 旧版本作业启动失败或保存点失败 | 场景作者 |
| 恢复 | 当前 dev 版本无法恢复保存点 | PR 作者 / 模块负责人 |
| 续跑 | 作业恢复但无法继续处理 | PR 作者 / 模块负责人 |
| 恢复后验证 | 断言作业在恢复的输出上失败 | PR 作者 / 模块负责人 |

运行脚本已记录到阶段特定的文件（`old-server.log`、`old-job.log`、`old-savepoint.log`、`current-restore.log`、`current-assert.log`）。第二阶段通过以下方式增强：

1. 添加**摘要制品**，清晰标记哪个阶段失败
2. 在 GitHub Actions UI 中添加**阶段级注解**（`::error::` / `::warning::`）
3. 添加断言结果的**差异输出**

### 6. 兼容性契约

工作流断言以下兼容性保证：

> 由 SeaTunnel 版本 N-1（最新稳定版本）创建的保存点可以被当前 `dev` 分支构建恢复，
> 作业将继续处理并产生经 Assert sink 验证的正确输出。

**流式场景验证**：对于流式（endless）场景，仅靠 Assert sink 无法证明恢复后的作业确实继续处理了数据，
因为恢复前已存在于 sink 目录中的数据可能满足断言条件。每个流式场景必须包含一个 `after_restore.sh` 钩子，
在保存点恢复后向源系统插入一个**恢复后标记**（例如一个包含已知值的行）。
Assert sink 配置必须验证该标记行存在，从而明确证明作业在恢复后继续处理了新数据。

此保证适用于：

- **Zeta 引擎**（本地模式）
- 启用了检查点的**流式作业**
- **兼容性矩阵中列出的场景**（而非所有连接器）

此保证不适用于：

- Flink / Spark 引擎恢复路径（尚未覆盖）
- 未在场景矩阵中表示的连接器
- 仅元数据升级（schema 变更、配置迁移）
- 早于 N-1 的版本创建的保存点（未测试）

### 7. 构件获取和可重现性

| 构件 | 来源 | 缓存 |
|------|------|------|
| 旧版本二进制文件 | Apache 镜像（downloads/archive） | 缓存在 `target/upgrade-compatibility/downloads/` |
| 当前 dev 二进制文件 | 从源码构建 `./mvnw package -pl seatunnel-dist -am` | 不缓存（始终重新构建） |
| 连接器 JAR（旧版本） | Maven Central（`dependency:get`） | 缓存在 `old-dist/connectors/` |
| Docker 镜像（MySQL、Kafka 等） | Docker Hub | 标准 Docker 层缓存 |

**可重现性规则**：

- 旧版本二进制文件通过 SHA-256 校验和标识，每次运行开始时记录
- 连接器 JAR 按精确版本解析（`<artifactId>:<version>`）
- Docker 镜像使用固定标签（如 `mysql:8.0`，而非 `mysql:latest`）

### 8. 分类和责任人

| 失败类型 | 分类流程 | 责任人 |
|---------|---------|-------|
| 场景特定不稳定 | 提交带 `CI&CD` 标签的 issue，分配给场景作者 | 场景作者 |
| 真正的兼容性回归 | 二分定位到问题提交，通知 PR 作者 | 兼容性工作流负责人 |
| 基础设施故障 | 检查 runner 日志，重新运行工作流 | CI 基础设施团队 |

**升级路径**：如果工作流在 `dev` 上连续失败超过 2 天，兼容性工作流负责人必须提交阻塞 issue
并升级到开发邮件列表。

## 实施计划

### 阶段 2a：文档和契约（本 PR）

- [x] 编写本 STIP 文档（英文 + 中文）
- [x] 更新 `tools/upgrade_compatibility/README.md` 以引用 STIP
- [ ] 将兼容性契约添加到 `docs/en/introduction/concepts/` 和 `docs/zh/introduction/concepts/`

### 阶段 2b：扩展场景覆盖

- [ ] 添加 `kafka-source-localfile` 场景
- [ ] 添加 `jdbc-sink-postgres` 场景
- [ ] 添加 `mysql-cdc-to-jdbc-sink` 场景

### 阶段 2c：PR 选择性触发

- [ ] 向工作流添加带路径过滤的 `pull_request` 触发器
- [ ] 以建议模式运行 4 周
- [ ] 提升为过滤路径的必选状态检查

### 阶段 2d：失败诊断和验证加固

- [ ] 在 GitHub Actions 中添加阶段级注解
- [ ] 添加摘要制品生成
- [ ] 添加断言差异输出
- [ ] 为所有流式场景添加 `after_restore.sh` 钩子以验证恢复后处理

## 考虑的替代方案

### 替代方案 A：每个 PR 运行完整矩阵

在每个 PR 上运行完整场景矩阵可以提供最大覆盖，但成本过高（每次运行 90+ 分钟，runner 成本高）。
渐进式门控模型在覆盖和实用性之间取得平衡。

### 替代方案 B：仅定时运行

仅依赖定时运行会漏掉运行间隔期间引入的回归。PR 选择性触发器在引入点附近捕获回归。

### 替代方案 C：第二阶段覆盖 Flink 引擎

添加 Flink 引擎恢复场景有价值，但会引入显著复杂性（Flink 集群设置、不同的保存点格式）。
推迟到第三阶段。

## 参考资料

- [第一阶段 PR #11301](https://github.com/apache/seatunnel/pull/11301)
- [Issue #11239](https://github.com/apache/seatunnel/issues/11239)
- [Issue #11353](https://github.com/apache/seatunnel/issues/11353)（检查点时间线）
- [Issue #11354](https://github.com/apache/seatunnel/issues/11354)（CDC 延迟可观测性）
- [Issue #10177](https://github.com/apache/seatunnel/issues/10177)（Flink 恢复）
- [Issue #11020](https://github.com/apache/seatunnel/issues/11020)（序列化兼容性）