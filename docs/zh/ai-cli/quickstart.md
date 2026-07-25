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

# 方式 A2：Bedrock 上的 OpenAI 系模型（仅支持 Responses API，如 gpt-5.6-terra）
export AI_PROVIDER=bedrock-mantle
export OPENAI_MODEL='openai.gpt-5.6-terra'

# 方式 B：Anthropic API
export AI_PROVIDER=anthropic
export ANTHROPIC_API_KEY=sk-ant-...

# 方式 C：OpenAI 或兼容 API
export AI_PROVIDER=openai
export OPENAI_API_KEY=sk-...
# export OPENAI_BASE_URL=https://...   # Azure OpenAI、DeepSeek、本地 vLLM 等
```

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
