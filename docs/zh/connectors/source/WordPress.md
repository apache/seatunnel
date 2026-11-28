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

import ChangeLog from '../changelog/connector-http-wordpress.md';

# WordPress

通过 [WordPress.com 文章 API](https://developer.wordpress.com/docs/api/1/get/sites/$site/posts/) 批量读取一个公开 WordPress.com 网站中已发布的文章。连接器仅向 `public-api.wordpress.com` 发送无需认证的 HTTPS 读取请求，不会修改网站，也不直接连接自行部署的 WordPress。

## 支持范围

- 仅支持有界批处理，作业并行度设为 1。
- 不支持私有站点、草稿、页面、评论、自定义文章类型、流式读取或 CDC。
- 时间窗口按**发布时间**过滤，不按更新时间过滤，也不捕获删除。
- 按文章 ID 升序读取。总数变化、页大小不符、ID 重复或乱序、响应格式错误、HTTP 错误（含 429）或超出页数上限将使作业失败。总数与顺序稳定也不能排除所有并发变更；这不是事务一致快照，建议读取较早且稳定的时间窗口。
- 重启作业会重读整个窗口，不保存远端分页进度，不保证恰好一次。非事务型下游在作业失败前可能已写入部分数据。需要安全重跑时，以站点和文章 ID 作为下游幂等键。
- Zeta 批任务仅在 `env` 中设置 `checkpoint.interval` 时启用周期引擎检查点。这些检查点不保存远端分页进度，恢复后的 reader 会重读配置的整个窗口。
- 每页响应全部校验后才开始输出；每接收一行就返回引擎，使已解码页面在限速时也能让出检查点注入机会。单次下游接收仍可能阻塞；检查点不会将可变的发布窗口变成远端快照。

## 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `site` | string | 是 | 无 | WordPress.com 站点数字 ID 或公开域名，不包含协议、路径、端口或查询参数。域名可能变更时优先使用站点 ID。 |
| `start_date` | string | 是 | 无 | 发布时间下界（含），使用秒精度 UTC ISO 8601，例如 `2005-09-01T00:00:00Z`。 |
| `end_date` | string | 是 | 无 | 发布时间上界（含），格式同上，不得早于 `start_date`。 |
| `page_size` | int | 否 | 50 | 每页请求的文章数量，范围为 1–100。 |
| `max_pages` | int | 否 | 1000 | 页数上限，范围为 1–10000；结果超限时作业失败，不会截断。 |

时间窗口包含两端。拆分导出时，请使用按秒且不重叠的窗口，或在下游去重。

## 输出字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `id` | BIGINT | 文章 ID，在当前站点内唯一。 |
| `slug` | STRING | 文章的 URL 别名。 |
| `title` | STRING | API 返回的展示标题，可能包含 HTML 实体。 |
| `url` | STRING | 公开文章 URL。 |
| `published_at` | TIMESTAMP | 转换为 UTC 的发布时间。 |
| `modified_at` | TIMESTAMP | 转换为 UTC 的最后修改时间。 |

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  WordPress {
    site = "en.blog.wordpress.com"
    start_date = "2005-09-01T00:00:00Z"
    end_date = "2005-09-30T23:59:59Z"
    page_size = 1
  }
}

sink {
  Console {}
}
```

## 变更日志

<ChangeLog />
