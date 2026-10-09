import ChangeLog from '../changelog/connector-http-splunk.md';

# Http-Splunk Source Connector

## 描述

`Http-Splunk` 连接器允许通过 HTTP Source 模式批量读取 Splunk REST API 端点的数据。它使用基于 Token 的认证，并支持通过 POST 请求访问同步搜索导出端点。

## 主要特性

* 基于 HTTP 从 Splunk REST API（`/services/search/v2/jobs/export`）摄取数据
* 通过 `Authorization` 请求头进行基于 Token 的认证
* 使用表单编码（form-urlencoded）参数映射搜索查询与输出格式
* 内存保护机制（`max_response_size_bytes`），在大数据量导出时保护 Worker 堆内存

## 选项

| 名称 | 类型 | 必填 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| url | String | 是 | - | Splunk REST API 搜索导出 URL（`/services/search/v2/jobs/export`） |
| api_key | String | 是 | - | Splunk 认证 Token（例如 `Splunk <token>`） |
| method | String | 否 | `POST` | HTTP 请求方法 |
| keep_params_as_form | Boolean | 否 | `true` | 以表单编码（form urlencoded）形式传递参数 |
| max_response_size_bytes | Long | 否 | `20971520` | 允许的最大 HTTP 响应大小（字节），超过后快速失败，以避免大数据量导出时内存无限增长。 |
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

## 大数据量导出与内存注意事项

Splunk 的导出端点可能返回任意大的结果集。由于该连接器会将完整响应缓冲在内存中，并通过中间字符串表示进行解析，导出时的峰值内存占用约为原始响应大小的数倍（约 2x–3x）。

* **默认限制：** `max_response_size_bytes` 选项默认为 **20MB**（`20971520` 字节）。
* **Worker 堆内存规划：** 请确保 Worker 容器/JVM 堆内存相对于该限制进行了合理的规划。如果您的搜索返回大量数据，请使用 Splunk 的 `earliest`/`latest` 参数缩小搜索时间窗口，或使用更小的 `head` 限制。
* **调优：** 只有在确认集群 Worker 有足够的堆内存余量可以安全承受更大导出的内存放大倍数后，才调整 `max_response_size_bytes`。

<ChangeLog />
