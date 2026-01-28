# LLM 贡献与协作指南（Apache SeaTunnel）

本指南旨在帮助 AI 助手（LLM / Agents）对 Apache SeaTunnel 代码库进行 **安全、一致且可验证** 的更改。它借鉴了成熟 Apache 项目的最佳实践，并结合了 SeaTunnel 的 **构建、测试、架构和文档规范** 进行适配。

## ⚠️ 关键要求：提议更改前必须验证

**Agent 在建议或最终确定更改之前，必须在本地运行验证命令。**

```bash
# 格式化代码（强制）
./mvnw spotless:apply

# 快速验证（强制）
./mvnw -q -DskipTests verify

# 单元测试（强烈推荐）
./mvnw test
```

未能满足这些要求可能会导致 PR 被拒绝。

## Git 提交信息规范

SeaTunnel 遵循 **严格的提交信息格式** 以保持清晰且可搜索的历史记录。

**格式**：

```
[Type][Module] Description
```

### Types (类型)

* `Feature`  – 新功能
* `Fix`      – Bug 修复
* `Improve`  – 现有行为的改进
* `Docs`     – 仅文档变更
* `Test`     – 测试用例或测试框架变更
* `Chore`    – 构建、依赖或维护任务

### Modules (模块)

* `Connector-V2`  – seatunnel-connectors-v2
* `Zeta`          – seatunnel-engine (Zeta 引擎)
* `Core`          – seatunnel-core
* `API`           – seatunnel-api
* `Transform-V2`  – seatunnel-transforms-v2
* `Format`        – seatunnel-formats
* `Translation`   – seatunnel-translation
* `E2E`           – seatunnel-e2e

### 示例

* `[Fix][Connector-V2] Fix MySQL source split enumeration bug`
* `[Fix][Zeta] Fix checkpoint timeout under heavy backpressure`
* `[Feature][Transform-V2] Add LLM transform plugin`
* `[Improve][Core] Optimize jar package loading speed`
* `[Docs] Update quick start guide`

## 代码库结构

```text
seatunnel/
├── seatunnel-api/              # 核心 API 定义
├── seatunnel-connectors-v2/    # Source & Sink 连接器（主要贡献区域）
├── seatunnel-transforms-v2/    # Transform 插件（包含 LLM 等）
├── seatunnel-engine/           # Zeta 引擎 & Web UI
├── seatunnel-core/             # 作业提交入口 & CLI
├── seatunnel-translation/      # Flink & Spark 适配层
├── seatunnel-formats/          # 数据格式处理（JSON, Avro 等）
├── seatunnel-e2e/              # 端到端集成测试
├── docs/                       # 文档 (en & zh)
└── config/                     # 默认配置
```

## 代码规范

### Java 后端

* **格式化**：Google Java Format (AOSP 风格)，由 Spotless 强制执行
* **导入**：
    * 禁止通配符导入
    * 使用 shaded 依赖：`org.apache.seatunnel.shade.*`
* **空值处理**：避免隐式的空值假设
* **可见性**：保持 API 最小化；尽可能使用包级私有 (package-private)

### Apache License Header (强制)

所有 **新文件** 必须包含 ASF 许可证头：

```java
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
```

## 🚨 向后兼容性 (非常重要)

Agent 必须将向后兼容性视为 **硬性约束**。

* **禁止** 删除或重命名现有的配置选项
* **禁止** 随意更改默认值
* **禁止** 破坏公共 API 或 SPI 契约

任何不兼容的更改必须：

* 在文档中明确说明
* 包含迁移指南
* 在 PR 描述中清晰解释

## 依赖规则

* **禁止** 引入新依赖，除非绝对必要
* 优先使用 `org.apache.seatunnel.shade.*` 下的现有 shaded 依赖
* 任何新依赖必须：
    * 在 PR 描述中说明理由
    * 考虑 shading、大小和冲突风险

## 架构指南

### Connector (V2)

* 实现 `SeaTunnelSource` 或 `SeaTunnelSink`
* 使用 `Option` 定义配置
* 通过 `SourceSplitEnumerator` 支持并行处理
* 避免连接器特定逻辑泄漏到 engine 或 core 中

### Zeta Engine

* **Client**：提交作业配置
* **Master**：调度与协调
* **Worker**：执行任务 (Source → Transform → Sink)

请尊重任务边界和生命周期语义。

## 配置 (Option) 规则

* 所有面向用户的配置必须使用 `Option` 定义
* 每个选项必须包含：
    * 名称 (name)
    * 类型 (type)
    * 默认值 (default value，如果适用)
    * 清晰的描述 (description)
* 选项名称是 **稳定的契约**，不得随意重命名

## 错误处理与日志

* 异常必须包含足够的上下文（表名、任务、配置键）
* 避免吞掉异常
* 使用正确的日志级别：
    * INFO  – 生命周期事件
    * WARN  – 可恢复的问题
    * ERROR – 任务失败错误
* **严禁** 记录敏感信息（密码、令牌、凭证）

## 文档规则

* 任何用户可见的更改必须更新：
    * `docs/en`
    * `docs/zh`
* 配置名称、默认值和示例必须与代码完全匹配
* 文档是功能的一部分，而非事后补充

## 测试指南

### 单元测试

* 位于 `src/test/java` 下
* 验证行为，而非实现细节
* 倾向于确定性和最小化的测试

命令：

```bash
./mvnw test
```

### E2E 测试

* 位于 `seatunnel-e2e` 下
* 使用 Testcontainers
* 继承 `TestSuiteBase`

命令：

```bash
./mvnw -DskipUT -DskipIT=false verify
```

## 性能意识

Agent 必须考虑性能影响：

* 避免在热点路径中进行不必要的对象创建
* 对大内存缓冲区保持谨慎
* 考虑并行度和资源使用

## PR 范围规则

* 保持更改最小且专注
* 避免不相关的重构或仅格式化的更改
* 一个 PR 应该解决 **一个问题**

## 运行与调试

### 从源码构建

```bash
./mvnw clean install -DskipTests -Dskip.spotless=true
```

### 安装连接器

```bash
sh bin/install-plugin.sh 2.3.13
```

### 运行作业 (Zeta)

```bash
sh bin/seatunnel.sh --config config/v2.batch.config.template -e local
```
