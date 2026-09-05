---
sidebar_position: 2
---

# 快速开始

## 前置条件

- Python 3.10+（推荐 3.11 或 3.12）
- macOS、Linux 或 Windows WSL
- 任意一种 LLM 提供商凭证：
  - **AWS Bedrock** —— AWS 凭证（profile、环境变量或 IAM 角色）
  - **Anthropic API** —— `ANTHROPIC_API_KEY`
  - **OpenAI API**（或兼容 API）—— `OPENAI_API_KEY`
  - **OrcaRouter** —— `ORCAROUTER_API_KEY`
- （可选）SeaTunnel 安装目录，用于引擎级校验和作业执行

## 安装

### 从 SeaTunnel 发行版启动（推荐）

Shell 包装脚本首次运行时会自动安装 Python 依赖：

```bash
# 首次运行 —— 自动安装依赖并进入交互式初始化
bin/seatunnel-ai.sh --init

# 初始化完成后直接启动
bin/seatunnel-ai.sh
```

`SEATUNNEL_HOME` 会自动指向发行版根目录。

### 从源码安装

```bash
cd seatunnel-cli
bash setup.sh          # 安装全部提供商依赖 + 开发工具
seatunnel --init       # 交互式配置提供商
```

## 配置 LLM 提供商

```bash
# 方式 A：AWS Bedrock（默认）
export AI_PROVIDER=bedrock
export AWS_REGION=us-east-1

# 方式 A2：Bedrock 上的 OpenAI 系模型（bedrock-mantle，完整说明见下方专节）
export AI_PROVIDER=bedrock-mantle
export OPENAI_MODEL='openai.gpt-5.6-terra'

# 方式 B：Anthropic API
export AI_PROVIDER=anthropic
export ANTHROPIC_API_KEY=sk-ant-...

# 方式 C：OpenAI 或兼容 API
export AI_PROVIDER=openai
export OPENAI_API_KEY=sk-...
# export OPENAI_BASE_URL=https://...   # Azure OpenAI、DeepSeek、本地 vLLM 等

# 方式 D：OrcaRouter AI 网关
export AI_PROVIDER=orcarouter
export ORCAROUTER_API_KEY=orc_...
# 模型 ID 使用 provider/model 命名空间（如 deepseek/deepseek-v4-pro）；
# `orcarouter/auto` 会自动评级并路由每个请求。
# export ORCAROUTER_MODEL=orcarouter/auto
# export ORCAROUTER_SMALL_FAST_MODEL=orcarouter/auto
# export ORCAROUTER_ECHO_REASONING_CONTENT=true   # 可选：保留并回传推理模型的 reasoning_content
```

### OrcaRouter AI 网关

[OrcaRouter](https://www.orcarouter.ai) 是一个 OpenAI 兼容的 AI 网关，在单个端点
（`https://api.orcarouter.ai/v1`）之后暴露众多模型——Claude、GPT、Gemini、
DeepSeek、Qwen 等。模型 ID 使用 `provider/model` 命名空间，特殊的
`orcarouter/auto` 模型会自动为每个请求选择最佳模型。作为一等提供商配置：

```bash
# 需要 openai 包（复用 ".[openai]" extra）
pip install -e ".[openai]"

export AI_PROVIDER=orcarouter
export ORCAROUTER_API_KEY=orc_...
# export ORCAROUTER_MODEL=deepseek/deepseek-v4-pro    # 可选覆盖
# export ORCAROUTER_SMALL_FAST_MODEL=orcarouter/auto  # 可选覆盖
# export ORCAROUTER_ECHO_REASONING_CONTENT=true       # 可选：回传 reasoning_content

seatunnel "Sync MySQL users table to S3 Parquet"
```

该提供商使用 OpenAI Chat Completions 协议，因此完全支持 CLI 内部的工具调用循环
（规划期间的连接器查询）、流式输出、多轮会话，以及兼容推理模型的
reasoning_content 回放。

### bedrock-mantle：Bedrock 上的 OpenAI 系模型

Bedrock 上的部分 OpenAI 模型（如 `openai.gpt-5.6-terra`、`openai.gpt-5.6-sol`）
不在 Bedrock 基础模型目录中，只支持专用 `bedrock-mantle` 端点上的 OpenAI
**Responses API**——常规 `bedrock` 提供商（Converse API）和 `openai` 提供商
（Chat Completions）都无法调用它们。请使用 `bedrock-mantle` 提供商：

```bash
# 1. 安装提供商依赖（openai SDK >= 2.45 + AWS token 生成器）
pip install -e ".[bedrock-mantle]"

# 2. 配置——只需 AWS 凭证，不需要 OpenAI 账号或 API key
export AI_PROVIDER=bedrock-mantle
export AWS_REGION=us-east-1                    # us-east-1 / us-east-2 / us-west-2
export OPENAI_MODEL='openai.gpt-5.6-terra'     # 不设置时的默认值
# export OPENAI_SMALL_FAST_MODEL='openai.gpt-5.6-terra'

# 3. 正常生成
seatunnel "把 MySQL 的 users 表同步到 S3，Parquet 格式"
```

提供商契约：

- **端点**：`https://bedrock-mantle.{region}.api.aws/openai/v1`——这类模型
  要求的专属 `openai/v1` 路径（通用的 `v1` Responses 路径会拒绝这些模型）。
- **认证**：通过 `aws-bedrock-token-generator` 从你的 AWS 凭证（profile、
  环境变量或 IAM 角色）自动派生短期 bearer token，每 30 分钟自动轮换，
  不在任何地方存储长期密钥。
- **数据留存**：所有请求携带 `store=false`，Bedrock 不会在服务端留存你的
  提示词和生成的配置（服务默认行为是保留 30 天）。
- **参数**：这类模型不接受 `temperature`，提供商不会发送该参数，配置的
  temperature 值不会生效。
- **错误处理**：截断（`incomplete`）、失败和拒答的响应会抛出显式错误，
  而不是伪装成正常结果返回。

该提供商完整支持 CLI 内部的工具调用循环（规划阶段的连接器查询）和多轮
会话，包括工具调用之间模型推理输出（reasoning）的保留与回放。

API 密钥只从环境变量读取——绝不写入任何配置文件。

## 生成第一条管道

### 单发模式

```bash
seatunnel "Sync MySQL users table to S3 Parquet"
seatunnel "从 Kafka 读取订单数据写入 ClickHouse" -o my_job.conf
```

### 交互模式

```bash
seatunnel
```

```
🐬 SeaTunnel > 把 PostgreSQL 的 orders 表同步到 Doris

  📋 已生成 SeaTunnel 配置
  配置已保存至: .data/last_job.conf

🐬 SeaTunnel > 加个过滤，只保留 amount > 100 的订单

  📋 已生成 SeaTunnel 配置（已更新）

🐬 SeaTunnel > /check
  [1] 本地校验: PASS
  [2] 引擎 --check: PASS
  Dry-run 通过 —— 配置可以执行。

🐬 SeaTunnel > /run
  作业已提交: 1234567890 (orders-sync)
  状态: FINISHED
```

### 常用命令

| 命令 | 说明 |
|------|------|
| `/check` | 校验最近生成的配置；失败时自动诊断修复 |
| `/run` | 通过 REST API 或 `seatunnel.sh` 执行；失败时自动修复 |
| `/connectors` | 列出可用的 source、sink 和 transform |
| `/remember <内容>` | 记住非敏感信息（主机、端口、库名等） |
| `/sessions`、`/resume` | 查看和恢复历史会话 |

## 提问技巧

- **在描述里带上连接信息**（主机、端口、库名、表名）：生成的配置可以直接运行，而不是一堆占位符。
- **明确批处理还是实时**（"一次性全量拷贝" vs "持续捕获变更"）——这决定 BATCH/STREAMING 模式和是否选用 CDC 连接器。
- **凭证默认占位**：生成的配置用 `${MYSQL_PASSWORD}` 这类环境变量引用密码，`/run` 前先 export。
- 对于模型实测偏弱的场景（条件路由拆流、PostgreSQL-CDC 前置配置、Doris/StarRocks 选项——见[基准测试](benchmark.md)），生产使用前建议人工复核生成的配置。
