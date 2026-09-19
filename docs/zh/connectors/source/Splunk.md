import ChangeLog from '../changelog/connector-http-splunk.md';

# Http-Splunk Source Connector

## 描述

`Http-Splunk` 连接器基于 HTTP source 模式，从 Splunk REST API 端点批量读取数据。它使用基于 Token 的认证，并通过 POST 请求支持同步搜索导出（search export）端点。

## 主要功能

* 基于 HTTP 从 Splunk REST API（`/services/search/v2/jobs/export`）摄取数据
* 通过 `Authorization` 请求头进行 Token 认证
* 搜索查询与输出格式使用表单编码（form-urlencoded）参数映射
* 内存保护机制（`max_response_size_bytes`），在大结果集导出时快速失败，保护 Worker 堆内存

## 选项

| 名称 | 类型 | 是否必需 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| url | String | 是 | - | Splunk REST API 搜索导出 URL（`/services/search/v2/jobs/export`） |
| api_key | String | 是 | - | Splunk 认证 Token（例如 `Splunk <token>`） |
| method | String | 否 | `POST` | HTTP 请求方法 |
| keep_params_as_form | Boolean | 否 | `true` | 将参数保持为表单编码（form urlencoded） |
| max_response_size_bytes | Long | 否 | `20971520` | 允许的最大 HTTP 响应字节数，超过后快速失败，以避免大结果集导出导致内存无限增长。 |
| params | Map | 是 | - | 请求参数，包括 `search` 和 `output_mode` |

## 示例配置

```hocon
source {
  Splunk {
    url = "https://your-splunk-instance:8089/services/search/v2/jobs/export"
    api_key = "Splunk your_splunk_auth_token"
    method = "POST"
    keep_params_as_form = true
    max_response_size_bytes = 20971520
    params {
      search = "search index=_internal | head 10"
      output_mode = "json"
    }
    plugin_output = "splunk_data"
  }
}
```

## 大结果集导出与内存注意事项

Splunk 的导出端点可能返回任意大的结果集。由于该连接器会将完整响应缓冲在内存中，并通过中间字符串表示进行解析，导出期间的峰值内存约为原始响应大小的 2–3 倍。

* **默认限制：** `max_response_size_bytes` 默认为 **20MB**（`20971520` 字节）。
* **Worker 堆内存规划：** 请确保 Worker 容器/JVM 堆大小与该限制匹配。如果搜索返回大量数据，请使用 Splunk 的 `earliest`/`latest` 参数缩小搜索时间窗口，或使用更小的 `head` 限制。
* **调优：** 只有在确认集群 Worker 有足够的堆内存余量以安全应对更大导出带来的内存放大后，才调整 `max_response_size_bytes`。

<ChangeLog />
