import ChangeLog from '../changelog/connector-http-babelforce.md';

{/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */}

# Babelforce

安装 `connector-http-babelforce` 后，在并行度为 1 的有界作业中使用 `Babelforce`。`calls` 资源通过 `GET /api/v2/calls/reporting/simple` 读取。请选择账户所在区域。该接口支持双访问密钥请求头和 OAuth Bearer 认证。 [Babelforce Manager API](https://apps.babelforce.com/developer-hub/manager/)

## 输出

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `call_id` | STRING | 响应中的通话 ID。 |
| `parent_id` | STRING | 供应商返回的子通话父级 ID，缺少时为 null。 |
| `date_created` | STRING | 规范化后的 UTC 创建时间，保留可用的秒以下精度。 |
| `payload` | STRING | 以 UTF-8 JSON 表示的完整通话对象，保留嵌套字段、录音引用、空值和精确数值。 |

输出为 INSERT 记录。录音引用按元数据保留。

## Source 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `region` | `String` | `services` | Babelforce 区域，可用值为 services、us-east 和 ap-southeast。 |
| `auth_mode` | `String` | `access_key` | access_key 使用访问 ID 与令牌请求头，bearer 使用已有的 OAuth 令牌。 |
| `access_key` | `String` | — | 访问 ID。在 access_key 模式下必填。 |
| `token` | `String` | — | 必填。访问令牌或预先取得的 OAuth Bearer 令牌。 |
| `date_created_from` | `Long` | `0` | 创建窗口的起始 Unix 秒，包含边界。 |
| `date_created_to` | `Long` | 当前 Unix 秒 | 创建窗口末尾的 Unix 秒，包含边界。省略时，在首次创建分片时固定。 |
| `window_parameters` | `String` | `date_created` | date_created 使用 Manager OpenAPI 的参数，time 使用供应商发布的 Go SDK 参数。 |
| `page_size` | `Integer` | `102` | 每页请求的最大记录数。范围为 1 至 102。 |
| `max_pages` | `Integer` | `10000` | 每次扫描的最大分页数。范围为 1 至 20,000。 |
| `max_attempts` | `Integer` | `4` | 每个可重试分页请求的总尝试上限。范围为 1 至 10。 |
| `request_timeout_ms` | `Integer` | `30000` | 分页请求的总时限，包含重试与解码。范围为 100 至 300,000 毫秒。 |
| `max_response_bytes` | `Integer` | `8388608` | 解压后的响应字节上限。范围为 1,024 至 67,108,864。 |
| `endpoint` | `String` | 区域 HTTPS URL | 可选的可信自定义 HTTPS 源站，需同时设置 trust_certificate。 |
| `trust_certificate` | `String` | — | 自定义源站的工作节点本地 PEM 信任证书。连接时仍校验主机名。 |

## 日期窗口与恢复

- 默认 `window_parameters = date_created`，发送 `dateCreated.start` 和 `dateCreated.end`。如果环境使用供应商发布的 SDK 参数 `time.start` 和 `time.end`，请选择 `time`。这两种参数均使用 Unix 秒。连接器会核对每条通话记录是否位于请求的创建秒窗口。带时区偏移的时间转换为 UTC，无偏移的时间按 UTC 解释。 [Babelforce Manager 规范](https://apps.babelforce.com/developer-hub/manager/) [Babelforce SDK](https://github.com/babelforce/manager-sdk-go)
- 检查点保留请求的结束时间与对应页码。连接器按供应商返回的总页数读取，包括中间的空页，并核对响应中的页码、总条数和续页信息。首页未返回总页数且其他元数据一致时，按单页处理。所有请求都在本地构造，续页路径或位置与预期矛盾时会失败。
- 页内恢复时，在输出剩余记录前核对重新读取的响应摘要。数据或读取范围变化可能需要重新扫描。供应商可能调整分页中的顺序。按创建时间读取无法覆盖所有后续更新或清除，请在下游按源身份和通话 ID 对重叠扫描去重或合并。

## 示例

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}
source {
  Babelforce {
    region = "services"
    access_key = ${BABELFORCE_ACCESS_ID}
    token = ${BABELFORCE_ACCESS_TOKEN}
    date_created_from = 1730592000
    plugin_output = "calls"
  }
}
sink {
  Console {
    plugin_input = "calls"
  }
}
```

<ChangeLog />
