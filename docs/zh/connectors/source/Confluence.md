import ChangeLog from '../changelog/connector-http-confluence.md';

# Confluence

<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

## 描述

Confluence Source 通过 [Confluence Cloud REST API](https://developer.atlassian.com/cloud/confluence/rest/v2/intro/) 执行有界批量读取。每个 Source 配置块选择一种资源。需要读取多种资源时，可为每种资源分别配置一个 Source。

| 资源 | API | 输出 |
| --- | --- | --- |
| `pages` | REST v2 | 账号可查看的页面，包含所选格式的正文 |
| `blog_posts` | REST v2 | 账号可查看的博客文章，包含所选格式的正文 |
| `space` | REST v2 | 账号可查看的空间，并在服务端提供时返回纯文本描述和图标信息 |
| `group` | REST v1 | 具有站点访问权限的账号可查看的用户组 |
| `audit` | REST v1 | 具有 Confluence Administrator 全局权限的账号可读取的留存审计事件 |

## 安装与认证

通过对应 SeaTunnel 版本的插件安装工具安装 `connector-http-confluence`。连接使用标准 TLS 证书与主机名校验。创建 Atlassian 传统 API token，并将其对应账号的登录邮箱配置为 `username`。通过部署环境的密钥机制提供 token。 [Atlassian Basic 认证](https://developer.atlassian.com/cloud/confluence/basic-auth-for-rest-apis/)

## 参数

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| site_url | String | 是 | - | 可信的 HTTPS 站点地址，例如 `https://example.atlassian.net`。地址无需包含 `/wiki`。 |
| username | String | 是 | - | Atlassian 账号登录邮箱。 |
| token | String | 是 | - | 该账号的传统 Atlassian API token。 |
| resource | String | 是 | - | 支持 `pages`、`blog_posts`、`space`、`group` 或 `audit`。 |
| body_format | String | 否 | `storage` | 页面和博客正文格式，支持 `storage` 和 `atlas_doc_format`。 |
| page_size | Int | 否 | `25` | 每页请求条数，范围 1–100。服务端可以返回更少的记录。 |
| max_pages | Int | 否 | `10000` | 每次读取的页数上限，范围 1–50000。达到上限后仍有下一页时，任务失败。 |
| max_attempts | Int | 否 | `4` | 每页总请求次数，范围 1–10，包含首次请求。 |
| connect_timeout_ms | Int | 否 | `10000` | 连接超时，范围 1–300000 毫秒。 |
| read_timeout_ms | Int | 否 | `30000` | Socket 空闲超时，范围 1–300000 毫秒。 |
| page_timeout_ms | Int | 否 | `120000` | 单页总时限，包含重试及等待，范围 1–600000 毫秒。 |
| max_response_bytes | Int | 否 | `16777216` | 单页解压后响应大小上限，范围 1–33554432 字节。 |

Source 仅使用通过校验的游标或偏移量继续翻页。凭据只会发送至配置的站点，并拒绝重定向。可重试故障会重试同一页。服务端的 `Retry-After` 指定最短等待时间，剩余时限不足时该页失败。

## 输出与恢复

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| resource | String | 所选 API 资源名称。 |
| id | String | 提供方返回的记录 ID，可能为空。 |
| audit_created_at | Bigint | API 返回的审计事件 `creationDate`，其他资源或缺少此字段时为空。多个审计事件可以有相同的时间戳。 |
| content | String | API 返回的记录 JSON 对象，保留嵌套字段和空值。 |

每个 Source 使用 `job.mode = "BATCH"` 和 `parallelism = 1`。新任务从头全量读取。Zeta 从 checkpoint 或 savepoint 恢复时，会回到保存的请求位置，并校验该页已经输出的记录前缀。若已输出前缀发生变化或游标已失效，可重新启动全量任务。对于非事务型 Sink，恢复点之后已经输出的数据可能重放。读取过程中若远端集合有变更，分页结果也会变化。该 Source 不提供远端的时间点快照。

此 Source 支持使用传统 API token 的 Confluence Cloud。带范围限制的 token 和服务账号 token 使用单独的 Atlassian 网关，当前不支持。Data Center、评论、附件、组成员展开、写入、增量同步及 CDC 不在支持范围内。

## 示例

读取页面并输出到 Console。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Confluence {
    site_url = "https://example.atlassian.net"
    username = "reader@example.com"
    token = "replace-with-classic-api-token"
    resource = "pages"
  }
}
sink {
  Console {}
}
```

将页面和空间作为两张表分别读取。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Confluence {
    site_url = "https://example.atlassian.net"
    username = "reader@example.com"
    token = "replace-with-classic-api-token"
    resource = "pages"
    plugin_output = "confluence_pages"
  }
  Confluence {
    site_url = "https://example.atlassian.net"
    username = "reader@example.com"
    token = "replace-with-classic-api-token"
    resource = "space"
    plugin_output = "confluence_spaces"
  }
}
sink {
  Console {
    plugin_input = "confluence_pages"
  }
  Console {
    plugin_input = "confluence_spaces"
  }
}
```

<ChangeLog />
