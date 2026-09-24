---
sidebar_position: 1
---

# AI CLI 概览

SeaTunnel AI CLI 用自然语言生成可直接投产的 SeaTunnel 数据管道配置。用中文或英文描述一个数据同步任务，CLI 会生成经过校验的 HOCON 配置文件——内置自动校验、错误修复和一键执行。

```
🐬 SeaTunnel > 把 MySQL 的 users 表同步到 S3，Parquet 格式

  ⚙️  正在生成 SeaTunnel 配置...
  ✅ 校验配置（第 1 轮）...

  📋 已生成 SeaTunnel 配置
  配置已保存至: .data/last_job.conf
```

AI CLI 以 `seatunnel-cli` 模块的形式内置在 SeaTunnel 主仓库中，并随标准发行版一起打包。可通过发行版的 `bin/seatunnel-ai.sh` 启动，也可以从源码用 pip 安装。

## 核心能力

- **自然语言生成配置** —— 中英文输入，输出完整 HOCON 配置
- **多 LLM 提供商** —— AWS Bedrock（含通过 `bedrock-mantle` 端点接入的 OpenAI 系模型）、Anthropic API、OpenAI 及兼容 API、OrcaRouter AI 网关
- **多智能体流水线** —— Planner → 配置生成 → 校验 → 自动修复，最多 3 轮纠错
- **连接器知识库** —— 150+ 连接器的完整选项规则与取值约束，来自运行中引擎或内置元数据
- **校验与修复** —— 本地检查、引擎 `--check`/dry-run，`/check` 或 `/run` 失败时由 LLM 自动诊断修复
- **会话与记忆** —— 多轮对话细化配置、会话持久化、连接信息记忆（绝不存储凭证）

## 本节文档

| 页面 | 内容 |
|------|------|
| [快速开始](quickstart.md) | 安装、配置 LLM 提供商、生成第一条管道 |
| [设计思路](design.md) | 多智能体架构与校验管道的设计 |
| [模型基准测试](benchmark.md) | 7 个大模型的实测准确率、选型建议与已知弱场景 |

## 与其它 AI 工具的关系

AI CLI 是内置于运行时的工具。外部配套工具维护在 [SeaTunnel Tools](https://github.com/apache/seatunnel-tools) 仓库中：[SeaTunnel Skill](../tools/seatunnel-skill.md)（Claude 集成，IDE 级辅助）、[MCP 服务](../tools/seatunnel-mcp.md)（LLM 编程式访问 SeaTunnel 资源）、[x2seatunnel](../tools/x2seatunnel.md)（配置转换）。
