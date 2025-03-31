# SeaTunnel MCP 快速入门指南

本指南将帮助您快速开始使用 SeaTunnel MCP 服务器，并通过 Claude 等大型语言模型与 SeaTunnel 进行交互。

## 准备工作

确保您已安装以下软件：

- Python 3.9 或更高版本
- 运行中的 SeaTunnel 实例（或使用 Docker Compose）
- Claude Desktop（可选，如需与 Claude 集成）

## 安装

### 方法 1：直接安装

```bash
# 克隆仓库
git clone <仓库URL>
cd seatunnel-mcp

# 创建虚拟环境并安装
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -e .
```

### 方法 2：使用 Docker Compose

```bash
# 克隆仓库
git clone <仓库URL>
cd seatunnel-mcp

# 启动 SeaTunnel 和 MCP 服务器
docker-compose up -d
```

## 基本用法

### 1. 启动 MCP 服务器

```bash
# 使用命令行工具
seatunnel-mcp run

# 或直接运行模块
python -m src.seatunnel_mcp
```

### 2. 使用 MCP Inspector 测试

MCP Inspector 是一个用于测试 MCP 服务器的工具。通过它，您可以直接与 MCP 服务器交互，无需 Claude 或其他 LLM。

```bash
# 安装 Node.js 和 npm 后
npx @modelcontextprotocol/inspector python -m src.seatunnel_mcp
```

这将打开一个网页界面，您可以在其中测试各种 MCP 工具。

### 3. 与 Claude Desktop 集成

```bash
# 配置 Claude Desktop
seatunnel-mcp configure-claude
```

然后，重启 Claude Desktop，并在与 Claude 的对话中尝试使用 SeaTunnel。

## 常见使用场景示例

### 提交一个简单的作业

在 Claude 中，您可以这样与 SeaTunnel 交互：

> 请帮我提交一个从 HDFS 到 Elasticsearch 的 SeaTunnel 作业，将 users 表中的数据导入到 users-index 索引中。

Claude 将帮助您创建配置并提交作业，例如：

```
我将帮您提交一个从 HDFS 到 Elasticsearch 的作业。

首先，让我检查当前的连接设置。
[Claude 获取连接设置]

现在，我将提交作业配置。以下是我准备的配置：

env {
  job.mode = "batch"
}

source {
  Hdfs {
    path = "/data/users"
    format = "json"
  }
}

sink {
  Elasticsearch {
    hosts = ["http://elasticsearch:9200"]
    index = "users-index"
  }
}

[Claude 提交作业]

作业已成功提交！作业 ID 是 123456。您可以稍后查询这个作业的状态。
```

### 查看运行中的作业

> 请显示当前所有正在运行的 SeaTunnel 作业。

Claude 将使用 `get-running-jobs` 工具获取并展示所有运行中的作业。

### 停止一个作业

> 请停止作业 ID 为 123456 的作业。

Claude 将使用 `stop-job` 工具停止指定的作业。

## 高级用法

### 创建作业模板

您可以要求 Claude 记住某些常用的作业配置作为模板：

> 请记住这个配置作为 "hdfs-to-es" 模板：
> ```
> env {
>   job.mode = "batch"
> }
> source {
>   Hdfs {
>     path = "/data/${path}"
>     format = "${format}"
>   }
> }
> sink {
>   Elasticsearch {
>     hosts = ["${es_host}"]
>     index = "${index}"
>   }
> }
> ```

然后，您可以这样使用模板：

> 使用 "hdfs-to-es" 模板创建作业，参数是：path=users, format=json, es_host=http://elasticsearch:9200, index=users-index

### 动态切换 SeaTunnel 实例

如果您需要连接到不同的 SeaTunnel 实例：

> 请将连接切换到测试环境的 SeaTunnel，URL 是 http://test-seatunnel:8090

Claude 将使用 `update-connection-settings` 工具更新连接设置。

## 故障排除

如果您遇到问题：

1. 确保 SeaTunnel 实例正在运行并可访问
2. 检查 MCP 服务器的日志输出
3. 使用 MCP Inspector 测试各个工具是否正常工作
4. 检查环境变量和连接设置

详细的故障排除指南请参阅 [用户指南](USER_GUIDE.md) 的"故障排除"部分。

## 下一步

- 阅读 [用户指南](USER_GUIDE.md) 了解更多功能和选项
- 查看 [示例文件夹](../examples/) 获取更多示例配置
- 如果您想贡献代码，请参阅 [开发者指南](DEVELOPER_GUIDE.md) 