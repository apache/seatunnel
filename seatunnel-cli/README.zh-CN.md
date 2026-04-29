# SeaTunnel CLI

使用自然语言生成 [Apache SeaTunnel](https://seatunnel.apache.org/) 数据管道配置。

用中文或英文描述您的数据同步任务，CLI 即可生成生产级 HOCON 配置文件，并支持校验、自动修复和一键执行。

## 功能特性

- **自然语言转配置** -- 用中文或英文描述需求，即可获得有效的 SeaTunnel 配置
- **多 LLM 提供商** -- 支持 AWS Bedrock、Anthropic API、OpenAI（及兼容 API，如 Azure OpenAI）
- **多智能体流水线** -- 规划器 -> 生成器 -> 校验器 -> 自动修复，最多 3 轮纠错
- **100+ 连接器** -- 全面覆盖 SeaTunnel 连接器生态，支持运行时元数据反射
- **技能框架** -- 三层生成：技能 SOP -> 黄金示例 -> 连接器元数据
- **自动保存** -- 生成的配置自动保存到 `.data/last_job.conf`（与 CLI 同目录）
- **自动修复** -- `/check` 和 `/run` 失败时自动触发 LLM 诊断和配置修复
- **会话与记忆** -- 多轮对话，支持持久化会话历史和连接信息记忆
- **试运行校验** -- 本地语法检查 + 引擎 `--check` + REST API 校验
- **双语支持** -- 支持中英文自然语言输入

## 前置条件

- **Python 3.10+**（推荐 3.11 或 3.12）
- **pip**（Python 3.10+ 自带，用于自动安装依赖）
- **操作系统**：macOS、Linux 或 Windows 上的 WSL
- 以下 LLM 提供商之一：
  - **AWS Bedrock** -- 需要 AWS 凭证和 `boto3`
  - **Anthropic API** -- 需要 `ANTHROPIC_API_KEY` 和 `anthropic` 包
  - **OpenAI API** -- 需要 `OPENAI_API_KEY` 和 `openai` 包
- （可选）运行中的 Apache SeaTunnel 引擎，用于实时元数据和作业执行

> **注意：** 通过 `bin/seatunnel-ai.sh` 启动时，首次运行会自动安装 Python 依赖，无需手动 `pip install`。

## 安装

### 从 SeaTunnel 发行版安装（推荐）

无需手动安装。Shell 脚本会自动处理一切：

```bash
# 首次运行 — 自动安装依赖，然后启动交互式配置
bin/seatunnel-ai.sh --init

# 配置完成后，直接启动
bin/seatunnel-ai.sh
```

> **注意：** `bin/seatunnel-ai.sh` 会自动将 `SEATUNNEL_HOME` 设置为发行版根目录，无需手动配置。

### 从源码安装（开发用途）

```bash
cd seatunnel-cli

# 快速安装（安装所有提供商 + 开发工具）
bash setup.sh
```

然后配置 LLM 提供商：

```bash
seatunnel --init
```

#### 手动安装（setup.sh 的替代方案）

```bash
pip install -e ".[bedrock]"    # AWS Bedrock
pip install -e ".[anthropic]"  # Anthropic API
pip install -e ".[openai]"     # OpenAI API
pip install -e ".[all]"        # 所有提供商
pip install -e ".[dev]"        # 开发环境（所有提供商 + pytest、ruff）
```

`pip install` 完成后，`seatunnel` 命令即可全局使用。

## 配置

复制示例环境文件并配置您的提供商：

```bash
cp env.example.sh env.sh
# 编辑 env.sh 填入您的配置
source env.sh
```

### 提供商配置

#### 方案 A：AWS Bedrock（默认）

```bash
export AI_PROVIDER=bedrock
export AWS_DEFAULT_REGION=us-east-1
export AWS_REGION=us-east-1

# 模型覆盖（可选）
export ANTHROPIC_MODEL='us.anthropic.claude-sonnet-4-20250514-v1:0'
export ANTHROPIC_SMALL_FAST_MODEL='us.anthropic.claude-haiku-4-5-20251001-v1:0'

# 凭证：使用 AWS CLI 配置文件、环境变量或 IAM 角色
# export AWS_ACCESS_KEY_ID=...
# export AWS_SECRET_ACCESS_KEY=...
```

#### 方案 B：Anthropic API

```bash
export AI_PROVIDER=anthropic
export ANTHROPIC_API_KEY=sk-ant-...

# 模型覆盖（可选）
export ANTHROPIC_MODEL=claude-sonnet-4-20250514
export ANTHROPIC_SMALL_FAST_MODEL=claude-haiku-4-5-20251001
```

#### 方案 C：OpenAI API

```bash
export AI_PROVIDER=openai
export OPENAI_API_KEY=sk-...

# 模型覆盖（可选）
export OPENAI_MODEL=gpt-4o
export OPENAI_SMALL_FAST_MODEL=gpt-4o-mini

# 兼容 API 的自定义端点（Azure OpenAI、本地模型等）
# export OPENAI_BASE_URL=https://your-endpoint.openai.azure.com/
```

### SEATUNNEL_HOME

`SEATUNNEL_HOME` 是 Apache SeaTunnel 引擎的安装路径。CLI 使用它来：

- **连接器元数据** — 加载 `connector_metadata.json` 以获取准确的连接器选项规则（150+ 连接器）
- **`/check`** — 运行 `seatunnel.sh --check` 进行引擎级配置校验
- **`/run`** — 通过 `seatunnel.sh -e local` 或 REST API 执行作业

```bash
export SEATUNNEL_HOME=/path/to/apache-seatunnel
```

**默认行为：** 如果未设置 `SEATUNNEL_HOME`，CLI 会根据自身包位置自动检测。在发行版压缩包中，CLI 位于 `cli/seatunnel_cli/`，因此会向上解析两级到压缩包根目录：

```
apache-seatunnel-3.0.0/          <-- 自动检测为 SEATUNNEL_HOME
├── bin/seatunnel.sh
├── bin/seatunnel-ai.sh
├── cli/seatunnel_cli/            <-- CLI 包位置
│   └── connector_metadata.json
├── connectors/
└── lib/
```

对于源码安装（`setup.sh` / `pip install`），目录结构不匹配，需要手动设置 `SEATUNNEL_HOME`。

> 未设置 `SEATUNNEL_HOME` 时，CLI 仍可使用 LLM 知识生成配置，但 `/check`、`/run` 和运行时连接器元数据将不可用。

### SeaTunnel 引擎（可选）

如需实时连接器元数据和 REST API 作业提交，请启动 SeaTunnel 服务：

```bash
export SEATUNNEL_API_BASE=http://localhost:5801  # 默认值
```

引擎运行时，CLI 以**集群模式**运行，支持实时连接器元数据、引擎级校验和通过 REST API 直接提交作业。引擎不可用时，回退到**离线模式**，使用内置的连接器元数据。

### 环境变量

| 变量 | 是否必需 | 默认值 | 说明 |
|------|---------|--------|------|
| `AI_PROVIDER` | 否 | `bedrock` | LLM 提供商：`bedrock`、`anthropic` 或 `openai` |
| `AWS_REGION` | Bedrock 必需 | `us-east-1` | Bedrock 使用的 AWS 区域 |
| `ANTHROPIC_API_KEY` | Anthropic 必需 | -- | Anthropic API 密钥 |
| `OPENAI_API_KEY` | OpenAI 必需 | -- | OpenAI API 密钥 |
| `OPENAI_BASE_URL` | 否 | -- | OpenAI 兼容 API 的自定义端点 |
| `ANTHROPIC_MODEL` | 否 | 提供商默认值 | 覆盖主模型 ID |
| `ANTHROPIC_SMALL_FAST_MODEL` | 否 | 提供商默认值 | 覆盖快速模型 ID |
| `OPENAI_MODEL` | 否 | `gpt-4o` | OpenAI 提供商的主模型 |
| `OPENAI_SMALL_FAST_MODEL` | 否 | `gpt-4o-mini` | OpenAI 提供商的快速模型 |
| `SEATUNNEL_HOME` | 否 | 自动检测 | SeaTunnel 安装目录。发行版压缩包中自动检测；源码安装需手动设置 |
| `SEATUNNEL_API_BASE` | 否 | `http://localhost:5801` | SeaTunnel REST API 端点 |
| `SEATUNNEL_CLI_DATA` | 否 | `<cli-package>/.data/` | 覆盖 CLI 数据目录（会话、记忆、配置） |

## 使用方法

> **提示：** 使用 SeaTunnel 发行版中的 `bin/seatunnel-ai.sh`，或 pip 安装后使用 `seatunnel` 命令，两者等效。

### 交互模式

```bash
seatunnel
```

启动交互式 REPL，支持流式输出、命令历史、会话持久化和多轮对话。

### 单次模式

```bash
# 生成并显示配置
seatunnel "Sync MySQL users table to S3 Parquet"

# 生成并保存到文件
seatunnel "从 Kafka 读取订单数据写入 ClickHouse" -o my_job.conf

# 临时切换提供商
seatunnel "Read CSV files and write to Elasticsearch" --provider openai --model gpt-4o
```

### CLI 参数

```
seatunnel [request] [options]

位置参数：
  request                  自然语言请求（省略则进入交互模式）

选项：
  -o, --output PATH        将生成的配置保存到文件
  --provider PROVIDER      LLM 提供商：bedrock | anthropic | openai
  --model MODEL            覆盖主模型 ID
  --fast-model MODEL       覆盖快速模型 ID
  --sync-catalog PATH      从 SeaTunnel 源码重新生成连接器目录
  -V, --version            显示版本
  -h, --help               显示帮助信息
```

### 交互命令

| 命令 | 说明 |
|------|------|
| `/save <path>` | 将配置保存到自定义路径（生成时自动保存到 `.data/last_job.conf`） |
| `/check` | 试运行校验最近的配置；失败时自动诊断并修复 |
| `/run` | 通过 REST API 或 `seatunnel.sh` 执行最近的配置；失败时自动诊断 |
| `/connectors` | 列出所有可用的 Source、Sink 和 Transform |
| `/sessions` | 列出最近的对话会话 |
| `/resume [id]` | 恢复之前的会话 |
| `/new` | 开始新会话 |
| `/memory` | 显示已记忆的信息（连接详情、偏好设置） |
| `/remember <text>` | 保存信息到记忆（如连接字符串、偏好设置） |
| `/forget <id>` | 删除一条记忆 |
| `/clear` | 清除对话历史并开始新会话 |
| `/help` | 显示帮助面板 |
| `/quit` | 退出 |

## 示例

### MySQL 到 S3（批处理）

```
🐬 SeaTunnel > Sync the users table from MySQL to S3 as Parquet files

  ⚙️  正在生成 SeaTunnel 配置...
  ✅ 校验配置（第 1 轮）...

  📋 已生成 SeaTunnel 配置
  配置已保存到：.data/last_job.conf
```

### Kafka 到 ClickHouse（流处理）

```
🐬 SeaTunnel > 从 Kafka 的 orders topic 实时同步到 ClickHouse 的 orders 表

  📋 已生成 SeaTunnel 配置
  配置已保存到：.data/last_job.conf

🐬 SeaTunnel > /check
  [1] 本地校验：通过
  [2] 引擎 --check：通过
  试运行通过 — 配置已就绪，可以执行。

🐬 SeaTunnel > /run
  作业已提交：1234567890 (orders-sync)
  状态：运行中
  状态：已完成
  作业执行成功。
```

### 多轮对话与记忆

```
🐬 SeaTunnel > /remember MySQL host=10.0.1.100 port=3306 database=orders

  已保存 mem_01（类型：连接信息）

🐬 SeaTunnel > Sync PostgreSQL orders to Doris

  📋 已生成 SeaTunnel 配置
  配置已保存到：.data/last_job.conf

🐬 SeaTunnel > Add a filter to only include orders where amount > 100

  📋 已生成 SeaTunnel 配置（已更新）
  配置已保存到：.data/last_job.conf

🐬 SeaTunnel > /save production_job.conf
  配置已保存到：production_job.conf
```

> **安全提示：** `/remember` 会拒绝包含密码、API 密钥、令牌或其他凭证值的条目。仅存储非敏感信息（主机名、端口、数据库名、表名）。

### 失败自动修复

```
🐬 SeaTunnel > /run
  作业失败：缺少必需选项 'fs.s3a.endpoint'

  正在诊断并修复配置...

  🔧 已修复配置
  （已添加缺失的 S3 端点配置）
  配置已保存到：.data/last_job.conf

  使用 /check 校验，然后 /run 重试。
```

## 架构

### 多智能体流水线

```
用户输入（自然语言）
     |
     v
+-----------------+     +----------------------+
|  规划器智能体    |---->|  连接器知识库         |
|  （分析意图      |<----|  （工具）             |
|   + 查询信息）   |     +----------------------+
+--------+--------+
         | 计划 / 对话
         v
+-----------------+
|  配置智能体      |  生成 HOCON 配置
+--------+--------+
         | 配置
         v
+-----------------+     +----------------------+
|  校验器智能体    |---->|  本地校验             |
|  （语法 + 逻辑） |     |  + 引擎 --check      |
+--------+--------+     +----------------------+
         |
    通过？ ---- 是 --> 输出 + 自动保存
         |
         否（最多 3 轮）
         |
         v
+-----------------+
|  修复智能体      |  自动纠正错误
+-----------------+
         |
    /run 或 /check 失败
         |
         v
+-----------------+
|  修复智能体      |  诊断 + 修补配置
+-----------------+
```

### 连接器知识库

两级解析，智能回退：

1. **运行时 API** -- 从运行中的 SeaTunnel 引擎获取实时元数据（`/option-rules` 端点）。始终准确，零维护成本。
2. **内置元数据** -- `connector_metadata.json` 随 CLI 包分发。包含 150 个连接器的完整选项规则，通过 SeaTunnel 引擎反射导出。零 LLM Token 消耗。

### 记忆系统

CLI 跨会话记忆信息，以提高配置准确性：

- **项目上下文** -- 表名、数据库名、常用模式。
- **偏好设置** -- 并行度、格式、语言偏好。

记忆存储在本地 `.data/memory.json`（与 CLI 包同目录）。使用 `/remember` 添加信息，`/memory` 查看，`/forget` 删除。

### 校验流水线

| 阶段 | 方法 | 说明 |
|------|------|------|
| **阶段 1** | 本地校验 | HOCON 语法、结构、必需参数、括号匹配、安全检查 |
| **阶段 2** | 引擎 `--check` | 调用 `seatunnel.sh --check` 进行引擎级校验 |
| **阶段 3** | REST API | 通过运行中的 SeaTunnel 集群进行可选校验 |
| **自动修复** | LLM 驱动 | 生成过程中最多 3 轮自动纠错 |
| **自动修复** | LLM 驱动 | `/check` 或 `/run` 失败时自动诊断和配置修补 |

## 连接器元数据

CLI 内置 `connector_metadata.json`（150 个连接器），通过运行时反射从 SeaTunnel 引擎导出，无需额外步骤。

如需为不同 SeaTunnel 版本重新导出（需要运行中的引擎）：

```bash
# 从运行中的引擎导出元数据
bin/seatunnel-metadata-export.sh
```

此命令调用引擎的选项规则反射 API，获取所有已注册连接器的信息，并将结果写入 `connector_metadata.json`。

## 许可证

基于 [Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0) 许可。
