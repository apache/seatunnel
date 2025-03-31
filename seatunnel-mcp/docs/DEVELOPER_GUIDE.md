# SeaTunnel MCP 开发者指南

本指南为 SeaTunnel MCP 项目的开发者提供指导，包括如何设置开发环境、代码贡献流程和测试方法。

## 开发环境设置

1. 克隆仓库并设置开发环境：

```bash
# 克隆仓库
git clone <repository_url>
cd seatunnel-mcp

# 创建并激活虚拟环境
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 安装开发依赖
pip install -e ".[dev]"
```

2. 安装预提交钩子（可选但推荐）：

```bash
pip install pre-commit
pre-commit install
```

## 项目结构

```
seatunnel-mcp/
├── docs/                 # 文档
├── examples/             # 示例配置文件
├── src/
│   └── seatunnel_mcp/    # 主要源代码
│       ├── __init__.py
│       ├── __main__.py   # 入口点
│       ├── client.py     # SeaTunnel API 客户端
│       ├── tools.py      # MCP 工具定义
│       └── schema.py     # 数据模型定义
├── tests/                # 测试
├── .env.example          # 环境变量示例
├── pyproject.toml        # 项目配置
└── README.md             # 项目说明
```

## 开发流程

### 添加新的工具

1. 首先在 `client.py` 中为 SeaTunnel API 添加相应的方法
2. 在 `tools.py` 中创建相应的 MCP 工具
3. 更新 `get_all_tools()` 函数以包含新工具
4. 添加单元测试

示例：添加一个新的工具来获取日志信息

```python
# 在 client.py 中
def get_job_logs(self, jobId: Union[str, int]) -> Dict[str, Any]:
    """获取作业日志信息。"""
    response = self._make_request("GET", f"/job-logs/{jobId}")
    return response.json()

# 在 tools.py 中
def get_job_logs_tool(client: SeaTunnelClient) -> Tool:
    """获取作业日志的工具。"""
    async def get_job_logs(jobId: Union[str, int]) -> Dict[str, Any]:
        return client.get_job_logs(jobId=jobId)

    return Tool(
        name="get-job-logs",
        description="获取指定作业的日志信息",
        fn=AsyncToolFn(get_job_logs),
        parameters=models.ParametersSchema(
            properties={
                "jobId": models.ParameterSchema(
                    type="string",
                    description="作业 ID",
                ),
            },
            required=["jobId"],
        ),
    )

# 更新 get_all_tools 函数
def get_all_tools(client: SeaTunnelClient) -> List[Tool]:
    return [
        # ... 现有工具
        get_job_logs_tool(client),
    ]
```

### 代码风格

- 使用 [Black](https://black.readthedocs.io/) 进行代码格式化
- 使用 [isort](https://pycqa.github.io/isort/) 排序导入
- 使用 [mypy](http://mypy-lang.org/) 进行类型检查
- 使用 [flake8](https://flake8.pycqa.org/) 进行代码风格检查

你可以使用以下命令执行这些检查：

```bash
# 代码格式化
black src tests

# 导入排序
isort src tests

# 类型检查
mypy src

# 代码风格检查
flake8 src tests
```

## 测试

### 单元测试

使用 pytest 运行单元测试：

```bash
pytest -xvs tests/
```

带覆盖率报告：

```bash
pytest --cov=src tests/
```

### 手动测试

使用 MCP Inspector 进行手动测试：

```bash
npx @modelcontextprotocol/inspector python -m src.seatunnel_mcp
```

### 集成测试

对于集成测试，你需要一个运行中的 SeaTunnel 实例。可以使用 Docker 启动一个测试实例：

```bash
docker run -d --name seatunnel -p 8090:8090 apache/seatunnel:latest
```

然后运行集成测试：

```bash
pytest -xvs tests/integration/
```

## 文档

- 所有公共函数、类和方法应有清晰的 docstring
- 遵循 [Google Python 风格指南](https://google.github.io/styleguide/pyguide.html)
- 保持 README.md 和用户指南的最新状态

## 提交 Pull Request

1. 确保所有测试通过
2. 更新相关文档
3. 如果添加了新功能，请同时添加相应的测试
4. 提交 PR 并在描述中详细说明变更内容

## 发布流程

1. 更新版本号（在 `__init__.py` 和 `pyproject.toml` 中）
2. 更新 CHANGELOG.md
3. 创建一个新的 git tag
4. 构建并上传到 PyPI：

```bash
python -m build
twine upload dist/*
```

## 问题与支持

如有问题或需要支持，可以：
- 提交 GitHub Issue
- 在 PR 中讨论
- 联系项目维护者 