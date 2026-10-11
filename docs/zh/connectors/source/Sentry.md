import ChangeLog from '../changelog/connector-sentry.md';

# Sentry

> Sentry 项目错误事件 Source 连接器。

## 描述

通过 `GET /api/0/projects/{organization}/{project}/events/` 读取单个项目的错误事件，用于外部数据仓库分析。现有 Sentry Sink 和 DSN 认证保持不变。

## 主要特性

- [x] 批处理
- [ ] 流处理
- [ ] 精确一次

仅支持 BATCH 模式和并行度 1，读取显式指定的时间范围。不支持项目发现、追踪、附件或一致性快照。
引擎注入的失败策略必须为 `FAIL_FAST`，单项目 Source 不支持继续处理其他表的策略。

## 前置条件

使用具有目标项目 `project:read` 权限的 Bearer Token，Sink DSN 不能用于读取事件。
区域服务或自托管部署通过 `api_base_url` 指定 HTTPS 源站，不能包含路径、查询参数、片段或末尾斜杠。
不跟随重定向，请直接配置最终 API 地址。默认验证 TLS 证书。
仅在 `mock_mode=true` 且 `token="mock-token"` 时允许 HTTP 测试服务，不得使用真实凭据。

参考 [事件 API](https://docs.sentry.io/api/events/list-a-projects-error-events/) 和 [分页协议](https://docs.sentry.io/api/pagination/)。

## 读取与恢复语义

`start_time` 和 `end_time` 必须是含时区偏移的绝对 RFC3339 时间，且开始早于结束。每页请求均携带规范化为 UTC 的时间范围。
固定发送 `full=false` 和 `sample=false`；`content` 是 API 返回的摘要事件，不包含完整堆栈。 `page_size` 对应 `per_page`。

仅在 Link 响应头中 `rel="next"` 且 `results="true"` 时读取下一页，`results="false"` 表示结束。
只提取游标，下一页始终使用已配置的源站、路径和时间范围。
缺失或非法分页头、循环游标、非法记录、重复 JSON 键、响应超限或超过 `max_pages` 均使作业失败，不会静默截断。

HTTP 429、500、502、503、504 和传输失败会进行有界重试。支持以秒数或 HTTP 日期表示的 `Retry-After`。
超过 60 秒的服务端等待要求会使作业失败，留待稍后重试，不会提前请求。认证失败和重定向不重试。
关闭 Reader 会中止正在执行的请求并唤醒重试等待。
超时参数控制 HTTP I/O 并为每次尝试安排中止。操作系统同步 DNS 解析、JSON 解析和重试等待不受严格的作业总时限约束。

恢复时重新读取整个时间范围，不对远端游标做检查点恢复。读取期间持有检查点锁，因此应选择合理时间范围和检查点超时。
失败前已写入非事务 Sink 的记录可能在重试后重复，必要时在下游去重。
数据保留策略、延迟写入、事件删除及分页期间数据变更可能影响结果，不保证完整历史、相邻范围无遗漏或快照一致性。

## 输出结构

固定结构，不支持自定义 `schema`。所有列均为 STRING，以保留标识符和时间戳表达。

| 列 | 可为空 | 含义 |
| --- | --- | --- |
| event_id | 否 | API 的 `eventID` |
| group_id | 是 | `groupID` |
| project_id | 是 | `projectID` |
| date_created | 是 | 原始 `dateCreated` |
| title | 是 | 事件标题 |
| message | 是 | 事件消息 |
| platform | 是 | 事件平台 |
| content | 否 | 完整返回事件对象序列化后的 JSON |

缺少可选字段时输出 null，类型不符则失败。JSON 数值保留十进制精度，但 `content` 不保证与响应逐字节一致。
事件可能包含个人数据，请限制目标存储访问并避免记录行内容。默认配置日志脱敏规则会隐藏 `token`。

## 参数

| 参数 | 类型 | 必填 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| token | STRING | 是 | - | 具有项目读取权限的 Bearer Token |
| organization | STRING | 是 | - | 组织 ID 或名称 |
| project | STRING | 是 | - | 项目 ID 或名称 |
| start_time | STRING | 是 | - | 绝对 RFC3339 开始时间 |
| end_time | STRING | 是 | - | 绝对 RFC3339 结束时间 |
| api_base_url | STRING | 否 | https://sentry.io | HTTPS 源站，不含路径及末尾斜杠 |
| page_size | INT | 否 | 100 | 每页请求记录数，1–100 |
| max_pages | INT | 否 | 10000 | 页数上限，1–100000；超限失败 |
| max_retries | INT | 否 | 3 | 额外重试次数，0–5 |
| retry_delay_ms | INT | 否 | 1000 | 最小重试间隔，1–60000 毫秒 |
| request_timeout_ms | INT | 否 | 30000 | 每次 HTTP 连接/读取超时及中止期限，1–120000 毫秒 |
| max_response_bytes | INT | 否 | 8388608 | 解压后响应字节上限，1024–16777216 |
| mock_mode | BOOLEAN | 否 | false | 仅允许使用假 Token 的 HTTP 测试服务 |

支持 `plugin_output` 等 [Source 通用参数](../common-options/source-common-options.md)。
组织和项目名称接受字母、数字、下划线和连字符。Source 不支持 `query`、`full`、`sample`、`dsn` 或自定义 schema。

## 示例

运行前请设置 `SENTRY_TOKEN` 和 `WAREHOUSE_PASSWORD` 环境变量。以下未加引号的 HOCON 替换表达式将在加载配置时解析为对应值。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
  Sentry {
    plugin_output = "events"
    token = ${SENTRY_TOKEN}
    organization = "example"
    project = "backend"
    start_time = "2026-01-01T00:00:00Z"
    end_time = "2026-01-02T00:00:00Z"
  }
}
sink {
  Jdbc {
    plugin_input = "events"
    url = "jdbc:postgresql://warehouse:5432/analytics"
    driver = "org.postgresql.Driver"
    user = "writer"
    password = ${WAREHOUSE_PASSWORD}
    database = "analytics"
    table = "sentry_events"
    generate_sink_sql = true
  }
}
```

请按上述八个字段创建兼容 STRING 类型的目标表并安装 JDBC 驱动。
集成测试使用确定性的 Sentry API 模拟服务运行引擎作业，不代表已验证所有 Sentry 云服务或自托管版本。

## 更新日志

<ChangeLog />
