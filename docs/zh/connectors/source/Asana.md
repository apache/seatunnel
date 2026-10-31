import ChangeLog from '../changelog/connector-http-asana.md';

# Http-Asana

> Asana 源连接器

## 描述

从 Asana REST API (`GET /tasks`) 读取单个项目的任务。基于 `connector-http-base` 构建。仅支持批处理模式。

## 主要特性

- [x] 批处理
- [ ] 流处理
- [ ] 精确一次
- [ ] 列投影
- [ ] 并行度

## 选项

| 名称 | 类型 | 是否必填 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| api_key | String | 是 | - | Asana 个人访问令牌（原始令牌，不含 `Bearer ` 前缀）。需要 `tasks:read` 权限。 |
| project_gid | String | 是 | - | 要读取其任务的项目。 |
| modified_since | String | 否 | - | 可选的 ISO 8601 时间戳，例如 `2026-10-01T00:00:00Z`。 |
| base_url | String | 否 | `https://app.asana.com/api/1.0` | Asana REST API 基础 URL。 |
| retry | Int | 否 | `3` | HTTP 429 和 5xx 错误的最大重试次数。 |
| retry_backoff_multiplier_ms | Int | 否 | `100` | 退避乘数（毫秒）。 |
| retry_backoff_max_ms | Int | 否 | `10000` | 最大退避时间（毫秒）。 |

### api_key
Asana 个人访问令牌（原始令牌，不含 `Bearer ` 前缀）。需要 `tasks:read` 权限。

### project_gid
要读取其任务的项目。

### modified_since
可选的 ISO 8601 时间戳，例如 `2026-10-01T00:00:00Z`。当任务自身的属性或关联发生变化时，Asana 会将其视为修改，但仅子任务发生变化时不计入。连接器在运行之间不保留状态。

### retry
HTTP 429 和 5xx（以及基础客户端中的 IOExceptions）的最大重试次数。退避时间从 `retry_backoff_multiplier_ms` 开始翻倍，最高可达 `retry_backoff_max_ms`。

## 输出列

所有列均为 STRING 类型：`gid`、`name`、`completed`、`completed_at`、`created_at`、`modified_at`、`due_on`、`assignee_gid`、`assignee_name`、`permalink_url`。
`completed_at`、`due_on` 以及负责人列可以为 null。

## 示例

```hocon
env {
  execution.parallelism = 1
  job.mode = BATCH
}

source {
  Asana {
    api_key = "<PAT>"
    project_gid = "1200000000000000"
    modified_since = "2026-10-01T00:00:00Z"
    plugin_output = "asana_tasks"
  }
}

sink {
  Console {
    plugin_input = "asana_tasks"
  }
}
```

## 注意事项

每次运行都从第一页开始读取项目。发生故障后，重启的任务会从头重新读取，并可能发出已经发送过的行；在运行期间编辑的任务也可能会出现两次。每一行都包含任务 `gid`，这可以协助幂等的下游 Sink 进行去重或更新（upsert），但连接器不提供送达保证。速率限制处理仅采用有界退避机制；不遵循 `Retry-After`。

<ChangeLog />
