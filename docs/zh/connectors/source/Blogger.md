<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

import ChangeLog from '../changelog/connector-http-blogger.md';

# Blogger

安装 `connector-http-blogger` 后，把 `Blogger` 配置为并行度 1 的批量 Source。每个实例通过 [Blogger v3 API](https://developers.google.com/blogger/docs/3.0/reference) 读取一种资源。在 Google OAuth 客户端启用 Blogger，使用通过离线访问授权获得的刷新令牌，授权范围为 `https://www.googleapis.com/auth/blogger.readonly`。

| 资源 | 记录 |
| --- | --- |
| `users` | 当前授权用户可访问的博客，由当前用户的博客列表接口返回。 |
| `blogs` | 每个博客的文章列表。 |
| `posts` | 单篇文章的详情，使用同时发现的博客 ID 和文章 ID。 |
| `pages` | 每个博客的页面。 |
| `comments` | 各博客下每篇文章的评论。 |

Source 自动发现上级资源，只输出选定的资源。读取结果受 Google 账号权限和 Blogger API 默认可见范围限制。每行包含 `blog_id`、`post_id` 和 `data`，类型均为 `STRING`。博客和页面记录的 `post_id` 为 null。`data` 是 JSON 对象，保留 API 的字段名、数字精度、嵌套值、null 和缺省字段。HTML、图片和 URL 字段保留在 JSON 中。连接器只请求 Blogger 和 Google OAuth 令牌服务。

## 选项

| 选项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `client_id` | string | — | 必填，Google OAuth 客户端 ID。 |
| `client_secret` | string | — | 必填，Google OAuth 客户端密钥。 |
| `token` | string | — | 必填，有离线 Blogger 读取权限的 Google OAuth 刷新令牌。 |
| `resource` | string | — | 必填，以上资源名之一。 |
| `page_size` | int | `100` | 分页资源每页请求的记录数，范围为 1–100。 |
| `max_pages` | int | `2000` | 每个列表的最大页数，范围为 1–10000。 |
| `max_blogs` | int | `5000` | 不分页的账号博客列表记录数上限，范围为 1–50000。 |
| `connect_timeout_ms` | int | `10000` | TCP 连接超时，范围为 1–120000 毫秒。 |
| `read_timeout_ms` | int | `30000` | Socket 读取超时，范围为 1–120000 毫秒。 |
| `request_deadline_ms` | int | `60000` | 单次分页读取的总时限，包括 OAuth 续期和重试，范围为 1–300000 毫秒。 |
| `max_attempts` | int | `4` | 单次 OAuth 或数据请求的最大尝试次数，范围为 1–10。 |
| `max_response_bytes` | int | `8388608` | 传输和解压后响应的独立上限，范围为 1024–33554432 字节。 |

连接器为过期的访问令牌续期，并在一次读取中对被拒绝的 Bearer 令牌最多再续期一次。连接器校验证书和主机名，拒绝重定向，并限制响应大小和重试等待。遇到错误格式、过期或重复的分页令牌时，无法安全续读的任务会明确失败。

每个新批任务进行全量读取。Zeta 检查点记录上级资源和数据的位置。恢复时，Source 在使用保存的行偏移前核对读取中的上级资源顺序和当前页。如果重取内容发生变化，或者 Google 拒绝保存的分页令牌，恢复会明确失败。供应商接口在读取过程中不提供账号级一致性快照。已完成的读取恢复后仍保持完成状态。变更 OAuth 主体、资源或页大小时应启动新的读取。

## 示例

```hocon
env {
    job.mode = "BATCH"
    parallelism = 1
}
source {
    Blogger {
        client_id = "example.apps.googleusercontent.com"
        client_secret = ${BLOGGER_CLIENT_SECRET}
        token = ${BLOGGER_REFRESH_TOKEN}
        resource = "comments"
    }
}
sink {
    Console {}
}
```

<ChangeLog />
