import ChangeLog from '../changelog/connector-http-zendesk.md';

# Zendesk

> Zendesk sink 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

用于向 [Zendesk REST API](https://developer.zendesk.com/api-reference/) 写入数据。使用 Zendesk 账户邮箱和 API 令牌进行认证（以 HTTP Basic `Authorization` 头发送），将记录写入 Zendesk 的端点，如工单（tickets）、用户（users）或组织（organizations）。

连接器会根据 URL 自动将每条记录包装在相应的 Zendesk 资源键中（例如，对于工单端点包装为 `{"ticket": {...}}`）。

## 主要特性

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## Sink 选项

| 名称                     | 类型    | 必填 | 默认值 | 描述 |
|--------------------------|---------|------|--------|------|
| url                      | String  | 是   | -      | 要写入的 Zendesk REST API 端点，例如 `https://your-subdomain.zendesk.com/api/v2/tickets`。 |
| email                    | String  | 是   | -      | 用于 API 令牌认证的 Zendesk 账户邮箱。与 `api_token` 组合为 `{email}/token:{api_token}`，作为 HTTP Basic `Authorization` 头发送。 |
| api_token                | String  | 是   | -      | Zendesk API 令牌。参见 [Zendesk API 令牌文档](https://support.zendesk.com/hc/en-us/articles/4408889192858) 了解如何生成。 |
| request_interval_ms      | int     | 否   | 100    | API 请求之间的最小间隔（毫秒）。默认 100ms。必须 `>= 0`。 |
| rate_limit_backoff_ms    | int     | 否   | 30000  | 收到 429（速率限制）响应时的基础退避时间（毫秒）。默认 30000ms。必须 `>= 0`。 |
| resource_key             | String  | 否   | -      | Zendesk API 请求体的 JSON 包装键（如 `"ticket"`、`"user"`、`"organization"`）。设置后将直接使用该值，而不从 URL 路径自动推断。适用于自动推断无法正确处理的端点。 |
| rate_limit_max_retries   | int     | 否   | 3      | 收到 429 响应后的最大重试次数。默认 3。必须 `>= 0`。 |
| common-options           |         | 否   | -      | Sink 通用选项。参见 [Sink 通用选项](../common-options/sink-common-options.md)。 |

## 使用说明

- `api_token` 是敏感信息。避免在共享的作业文件中硬编码真实令牌。请使用 SeaTunnel 变量替换或部署密钥机制。
- 连接器从 URL 路径推断 Zendesk 资源类型。例如，`/api/v2/tickets` 将每条记录包装为 `{"ticket": {...}}`，`/api/v2/users/create_or_update` 包装为 `{"user": {...}}`。如果自动推断对非标准端点产生了错误的键，可以通过 `resource_key` 显式指定。
- 每条输入记录作为单独的 API 调用发送。连接器不使用 Zendesk 批量端点。
- Zendesk 有速率限制（通常每分钟 400-700 个请求，取决于计划）。默认 `request_interval_ms = 100` 使单个连接器保持在该限制之内。当 `parallelism > 1` 时，连接器会自动将间隔乘以并行子任务数，确保所有 writer 的总请求速率保持在账户级别限制之内。配置 `rate_limit_backoff_ms` 和 `rate_limit_max_retries` 来控制连接器对 HTTP 429 响应的反应。
- Zendesk 创建端点在成功时返回 HTTP 201。连接器将 200 和 201 都视为成功。

## 任务示例

### 创建工单

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        subject = string
        status = string
        priority = string
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["帮我处理账单问题", "open", "normal"]
      },
      {
        kind = INSERT
        fields = ["无法登录", "open", "high"]
      }
    ]
  }
}

sink {
  Zendesk {
    url = "https://your-subdomain.zendesk.com/api/v2/tickets"
    email = "agent@example.com"
    api_token = "${ZENDESK_API_TOKEN}"
  }
}
```

### 创建或更新用户

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    schema = {
      fields {
        name = string
        email = string
      }
    }
    rows = [
      {
        kind = INSERT
        fields = ["Alice", "alice@example.com"]
      },
      {
        kind = INSERT
        fields = ["Bob", "bob@example.com"]
      }
    ]
  }
}

sink {
  Zendesk {
    url = "https://your-subdomain.zendesk.com/api/v2/users/create_or_update"
    email = "agent@example.com"
    api_token = "${ZENDESK_API_TOKEN}"
    request_interval_ms = 100
  }
}
```

## 更新日志

<ChangeLog />
