import ChangeLog from '../changelog/connector-http-sonarqube.md';

# SonarQube

> SonarQube Community Build 源连接器

`SonarQube` 源连接器通过 [SonarQube Web API](https://docs.sonarsource.com/sonarqube-community-build/extension-guide/web-api) 读取一个项目主分支的当前度量值。`GET /api/measures/component` 返回的每个度量值对应一行。

## 支持的特性

- 有界批量读取，源并行度为 `1`。不支持历史记录、增量或精确一次读取。
- 显式指定项目；不支持项目发现、规则或问题导出、拉取请求分析，也不支持选择分支。
- 服务端允许匿名浏览时，公开项目无需令牌。私有项目使用具有 Browse 权限的用户令牌，无需管理员权限。

## 选项

| 名称 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `base_url` | String | 是 | 服务基础地址，包含上下文路径（如有），例如 `https://sonar.example.org/sonarqube`。 |
| `project_key` | String | 是 | 精确的 SonarQube 项目标识，而不是显示名称。 |
| `metric_keys` | List of String | 是 | 1 到 50 个不重复的[度量标识](https://docs.sonarsource.com/sonarqube-community-build/user-guide/code-metrics/metrics-definition)。 |
| `token_env` | String | 否 | 存放 SonarQube 用户令牌的 Worker 环境变量名称。要求 HTTPS。不要把令牌写入任务配置。 |

## 输出

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `project_key` | STRING | 项目标识。 |
| `project_name` | STRING | 项目显示名称。 |
| `metric_key` | STRING | API 返回的度量标识。 |
| `value` | STRING | 当前整体度量值，按 API 文本原样输出；未返回时为 null。 |
| `best_value` | BOOLEAN | 当前结果是否为最优值；未指定时为 null。 |
| `measure` | STRING | 度量对象的紧凑 JSON，保留新代码周期字段（如有）。 |

API 未返回的度量不会被补行；空度量数组不产生输出。HTTP 错误、重定向或不一致的响应会使任务失败。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  SonarQube {
    base_url = "https://sonar.example.org/sonarqube"
    project_key = "example:app"
    metric_keys = ["ncloc", "coverage", "new_coverage"]
    token_env = "SONARQUBE_READ_TOKEN"
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
