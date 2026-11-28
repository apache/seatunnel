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

# Buzzsprout Source

`Buzzsprout` 以源端并行度 1 有界读取播客和单集元数据。发行包缺少此连接器时，请安装 `connector-http-buzzsprout`。

| 资源 | API 操作 | 范围 |
| --- | --- | --- |
| `podcasts` | `GET /api/podcasts` | 令牌所属用户有权访问的所有播客。 |
| `episodes` | `GET /api/{podcast_id}/episodes` | 指定播客的单集。 |

在 **Profile → API** 获取令牌。连接器发送 `Authorization: Token token=<token>` 和可识别的应用 User-Agent，并使用官方无后缀路径。播客和单集中的 URL 作为数据保留。 [Buzzsprout API](https://github.com/buzzsprout/buzzsprout-api) · [播客](https://github.com/buzzsprout/buzzsprout-api/blob/master/sections/podcasts.md) · [单集](https://github.com/buzzsprout/buzzsprout-api/blob/master/sections/episodes.md)

## 配置

| 参数 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `token` | 是 | | 个人资料中的 API 令牌值。 |
| `resources` | 是 | | 按请求顺序选取一个或多个资源，每个资源只出现一次。 |
| `podcast_id` | 读取 `episodes` 时 | | 正整数播客 ID，使用十进制字符串。 |
| `start_date` | 读取 `episodes` 时 | | 单集发布时间下界，包含该时刻。使用带时区的 RFC 3339 格式。 |
| `end_date` | 否 | 扫描开始时刻 | 单集发布时间上界，不包含该时刻，格式同上。扫描开始时冻结该值。 |
| `include_undated` | 否 | false | 是否输出 `published_at` 缺失或为 null 的单集。 |
| `max_attempts` | 否 | 4 | 每个列表的 HTTPS 总尝试次数，范围 1–10。 |
| `request_timeout_ms` | 否 | 30000 | 每个列表的总请求期限，含重试和解析，范围 100–300000 毫秒。 |
| `max_response_bytes` | 否 | 8388608 | 解压后响应大小上限，范围 1024–67108864 字节。 |
| `endpoint` | 否 | `https://www.buzzsprout.com` | 可选 HTTPS 源地址。显式配置时需同时指定 `trust_certificate`。 |
| `trust_certificate` | 否 | | 自定义 HTTPS 源地址所用的 Worker 本地 X.509 PEM 文件路径。 |

## 输出

`buzzsprout.records` 包含五个 `STRING` 列。

| 列 | 说明 |
| --- | --- |
| `resource` | `podcasts` 或 `episodes`。 |
| `record_id` | 用十进制文本精确表示供应商的正整数 ID。 |
| `podcast_id` | 播客 ID，来自播客记录或单集请求的配置。 |
| `published_at` | 统一为 UTC 的单集发布时间。播客行或无发布时间的单集行中，该字段为 null。 |
| `payload` | 完整供应商对象的 JSON 文本，保留嵌套字段、null 和数值精度。 |

单集按 `[start_date, end_date)` 筛选，公开状态和处理状态不参与筛选。记录的时间可能对应私人单集或待发布单集。显式上界可包含范围内的已排期单集。供应商返回的可见性、停用信息、媒体及联系字段都保留在 `payload` 中。

## 示例

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Buzzsprout {
    token = ${BUZZSPROUT_API_TOKEN}
    resources = ["podcasts", "episodes"]
    podcast_id = "140447"
    start_date = "2026-01-01T00:00:00Z"
    end_date = "2026-04-01T00:00:00Z"
  }
}

sink {
  Console {}
}
```

## 列表限制与恢复

两个列表接口均返回 JSON 数组。连接器校验整个数组后才发送数据，每个列表最多处理 50000 条，同时受配置的响应大小限制。协商使用 gzip 时按解压字节数检查。发现意外的续页响应头时，该列表请求失败。

检查点保存冻结的时间上界、资源位置、原始数组指纹和已发送的行数。列表内恢复会重新读取同一数组，并在发送剩余行前校验内容，包括已被时间范围过滤的记录。更换令牌、资源顺序、播客 ID、时间范围、无日期行设置、源地址或证书路径后需要重新扫描。稳定的响应支持恢复。供应商这两个接口没有跨资源的隔离快照或删除流。

HTTPS 客户端校验证书和主机名，禁止重定向，并在请求期限内重试符合条件的 GET。HTTP 429 使用有界指数退避，并遵守合法 `Retry-After` 给出的最短等待时间，支持小数秒。两个列表合计最多尝试 20 次。Buzzsprout 每分钟 60 次的配额适用于共享同一授权值的所有调用方。 [限流与重试](https://github.com/buzzsprout/buzzsprout-api#rate-limits-and-retries)
