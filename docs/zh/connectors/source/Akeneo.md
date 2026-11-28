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

import ChangeLog from '../changelog/connector-http-akeneo.md';

# Akeneo

安装 `connector-http-akeneo`，在批处理任务中配置 `Akeneo`。在 Akeneo PIM 中创建具备读取权限的连接，使用其客户端 ID、客户端密钥、API 用户名和密码。连接器使用私有连接的密码授权和令牌刷新。连接器校验 HTTPS 证书及主机名，并拒绝重定向。 [Akeneo 认证](https://api.akeneo.com/documentation/authentication.html)

## 资源

| 资源 | `/api/rest/v1` 下的 GET 路径 |
| --- | --- |
| `products` | `/products-uuid` |
| `categories` | `/categories` |
| `families` | `/families` |
| `family_variants` | `/families/{code}/variants` |
| `attributes` | `/attributes` |
| `attribute_groups` | `/attribute-groups` |
| `association_types` | `/association-types` |
| `channels` | `/channels` |
| `locales` | `/locales` |
| `currencies` | `/currencies` |
| `measure_families` | `/measurement-families`, `/measure-families` |

未配置 `resources` 时读取全部 11 种集合。只选 `family_variants` 也会查找产品族并读取每个产品族的变体，同时只输出变体行。产品读取使用 Akeneo SaaS 和 PIM 7 Community、Enterprise 文档中的 UUID 接口。当前度量接口返回数组。如果部署提供旧版分页度量接口，设置 `measurement_api = "legacy"`。可用端点和记录范围取决于版本及权限。 [REST API](https://api.akeneo.com/api-reference.html) · [PIM 7 接口](https://api.akeneo.com/api-reference-paas-index-70.html)

输出字段为 `resource STRING`、`id STRING`、可空的 `parent_id STRING` 和 `record STRING`。产品 ID 使用 UUID，其他资源使用 code。产品族变体的 parent_id 是产品族 code。record 保留供应商完整 JSON 对象，包括本地化和按渠道设置的值、嵌套数据、null 与数值精度。

## 参数

| 参数 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `base_url` | 字符串 | 必填 | PIM 的 HTTPS 源地址 |
| `client_id` | 字符串 | 必填 | 连接的客户端 ID |
| `client_secret` | 字符串 | 必填 | 连接的客户端密钥 |
| `username` | 字符串 | 必填 | 连接的 API 用户名 |
| `password` | 字符串 | 必填 | 连接的 API 密码 |
| `resources` | 字符串列表 | 全部 | 资源表中的资源名称，不可重复 |
| `measurement_api` | 字符串 | `current` | `current` 读取数组接口，`legacy` 读取旧版分页接口 |
| `page_size` | 整数 | `100` | 每页记录数，1–100 |
| `max_pages` | 整数 | `100000` | 每个集合或产品族的最大页数，1–100000 |
| `request_interval_ms` | 整数 | `25` | 请求之间的最短间隔，10–60000 毫秒 |
| `max_attempts` | 整数 | `4` | 允许重试的操作最多尝试次数，1–8 |
| `socket_timeout_ms` | 整数 | `15000` | 连接与读取空闲超时，100–120000 毫秒 |
| `page_deadline_ms` | 整数 | `60000` | 每页总时间上限，包括认证和重试，500–600000 毫秒 |
| `max_response_bytes` | 整数 | `8388608` | 解压响应大小限制，1024–33554432 字节。令牌响应另有 64 KiB 上限。 |

## 分页和恢复

产品使用 Akeneo 返回的不透明 search-after 游标，其他分页集合使用页号。连接器校验后续页的范围并拒绝循环。如果达到页数上限时仍有记录则报错。单个读取器控制请求速度。读取重试在每页总超时内处理有效的 Retry-After。令牌刷新中断时旧令牌可能已失效，因此令牌 POST 只在明确返回 HTTP 429 后重试。 [Akeneo 分页](https://api.akeneo.com/documentation/pagination.html) · [Akeneo 限流](https://api.akeneo.com/documentation/good-practices.html)

检查点保留资源、父页和子页位置以及部分处理页的校验值。恢复会重新认证，在跳过已处理行之前验证页面。恢复时保持源地址、客户端 ID、API 用户名、资源列表、度量接口与页大小一致，可以轮换密码和客户端密钥。目录变动可能影响后续页。每次新任务都从头扫描，不维护增量水位。

## 示例

```hocon
env {
  job.mode = "BATCH"
}

source {
  Akeneo {
    base_url = "https://your-pim.example.com"
    client_id = "${akeneo_client_id}"
    client_secret = "${akeneo_client_secret}"
    username = "${akeneo_api_username}"
    password = "${akeneo_api_password}"
    resources = ["products", "family_variants", "measure_families"]
  }
}

sink {
  Console {}
}
```

<ChangeLog />
