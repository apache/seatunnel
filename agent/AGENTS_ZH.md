# LLM 贡献与协作指南（Apache SeaTunnel）

本指南旨在帮助 AI 助手（LLM/Agents）在 SeaTunnel 代码库中进行安全、一致且可验证的更改。内容参考了成熟 Apache 项目的最佳实践，并结合 SeaTunnel 的构建、测试和文档规范进行了适配。

⚠️ **关键要求：提交前验证**
在提出更改之前，务必运行以下验证命令：
- **格式化代码**：`./mvnw spotless:apply`
- **快速验证**：`./mvnw -q -DskipTests verify`
- **运行单元测试**：`./mvnw test`

## Git 提交信息规范 (Git Commit Message Convention)
SeaTunnel 遵循严格的提交信息格式以保持历史记录清晰。
**格式**：`[Type][Module] Description`

**Types (类型)**：
- `Feature`：新功能
- `Fix`：Bug 修复
- `Improve`：现有功能的改进
- `Docs`：文档变更
- `Test`：测试用例或测试框架变更
- `Chore`：构建过程、依赖更新或维护任务

**Modules (模块)**：
- `Connector-V2`：`seatunnel-connectors-v2` 模块变更
- `Zeta`：`seatunnel-engine` (Zeta 引擎) 模块变更
- `Core`：`seatunnel-core` 模块变更
- `API`：`seatunnel-api` 模块变更
- `E2E`：`seatunnel-e2e` 模块变更
- `Transform-V2`：`seatunnel-transforms-v2` 模块变更
- `Format`：`seatunnel-formats` 模块变更
- `Translation`：`seatunnel-translation` 模块变更

**示例**：
- `[Fix][Connector-V2] Fix MySQL connector source split bug`
- `[Fix][Zeta] Fix checkpoint timeout issue`
- `[Feature][Transform-V2] Add LLM transform plugin`
- `[Improve][Core] Optimize jar package loading speed`
- `[Docs] Update quick start guide`

## 关键目录结构 (Key Directories)
```text
seatunnel/
├── seatunnel-api/              # 核心 API 定义
├── seatunnel-connectors-v2/    # Source & Sink 连接器 (主要贡献区域)
├── seatunnel-transforms-v2/    # Transform 插件 (包含 LLM 等)
├── seatunnel-engine/           # SeaTunnel Zeta 引擎 & Web UI
├── seatunnel-core/             # 作业提交入口 & CLI
├── seatunnel-translation/      # Flink & Spark 适配层
├── seatunnel-formats/          # 数据格式处理 (JSON, Avro 等)
├── seatunnel-e2e/              # 端到端集成测试
├── docs/                       # 文档 (en & zh)
└── config/                     # 默认配置
```

## 代码规范 (Code Standards)
**Java 后端**
- **风格**：Google Java Format (AOSP 风格)，由 Spotless 强制执行。
- **导入**：禁止通配符导入。对于 shaded 依赖（Guava, Jetty, Hikari, Janino, Commons-Lang3），必须使用 `org.apache.seatunnel.shade.*`。
- **License 头**：所有新文件必须包含标准的 Apache Software Foundation 许可证头。

**Apache 许可证头 (Apache License Headers)**
- **要求**：新文件必须包含 ASF 许可证头。
- **头内容**：
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

**文档**
- **双语**：用户可见的更改必须同时更新 `docs/en` 和 `docs/zh`。
- **一致性**：文档中的配置选项必须与代码实现保持一致。

## 架构模式 (Architecture Patterns)
**连接器 (Connectors V2)**
- 实现 `SeaTunnelSource` 或 `SeaTunnelSink`。
- 使用 `Option` 规则定义配置。
- 支持 `SourceSplitEnumerator` 以实现并行读取。

**引擎 (Zeta)**
- **Client**：提交作业配置到 Master。
- **Master**：调度任务到 Workers。
- **Worker**：执行任务 (Source -> Transform -> Sink)。

## 测试工具 (Test Utilities)
**单元测试 (Unit Tests)**
- 运行命令：`./mvnw test`。
- 位置：每个模块的 `src/test/java` 目录下。

**E2E 测试 (`seatunnel-e2e`)**
- 使用 Testcontainers 启动 Docker 环境。
- 定义测试用例需继承 `TestSuiteBase`。
- **运行命令**：`./mvnw -DskipUT -DskipIT=false verify` (运行集成测试，速度较慢)。

## 运行与调试 (Running & Debugging)
**从源码构建**
```bash
./mvnw clean install -DskipTests -Dskip.spotless=true
```

**安装连接器**
```bash
sh bin/install-plugin.sh 2.3.13  # 或指定版本
```

**运行作业 (Zeta)**
```bash
sh bin/seatunnel.sh --config config/v2.batch.config.template -e local
```
