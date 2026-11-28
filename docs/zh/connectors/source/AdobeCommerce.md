<!-- Licensed to the Apache Software Foundation (ASF) under one or more
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

import ChangeLog from '../changelog/connector-http-adobe-commerce.md';

# AdobeCommerce

`AdobeCommerce` 通过传统 Adobe Commerce 和 Magento Open Source REST v1 API 执行有界批量扫描。安装 `connector-http-adobe-commerce`，配置部署站点的 HTTPS 地址，填写该站点已接受且有选定资源读取权限的 Bearer Token。Adobe Commerce as a Cloud Service 使用独立的 IMS API。

连接器在部署地址后附加 `/rest/V1`，配置店铺视图代码时附加 `/rest/<store_code>/V1`。如果站点部署在子目录，请在 `base_url` 中填写该目录。TLS 证书必须匹配 URL 主机名。管理员令牌到期后请在连接器外更新，恢复时提供有效令牌。Adobe Commerce 2.4 默认禁用将集成访问令牌直接作为 Bearer Token 使用。[REST API 概述](https://developer.adobe.com/commerce/webapi/rest/) · [令牌认证](https://developer.adobe.com/commerce/webapi/get-started/authentication/gs-authentication-token)

## 资源

| 资源 | v1 下的 GET 路由 | 时间窗口字段 |
| --- | --- | --- |
| `carts` | `/carts/search` | `updated_at` |
| `orders` | `/orders` | `updated_at` |
| `coupons` | `/coupons/search` | `created_at` |
| `invoices` | `/invoices` | `updated_at` |
| `products` | `/products` | `updated_at` |
| `customers` | `/customers/search` | `updated_at` |
| `shipments` | `/shipments` | `updated_at` |
| `tax_rates` | `/taxRates/search` | 全量扫描 |
| `creditmemos` | `/creditmemos` | `updated_at` |
| `sales_rules` | `/salesRules/search` | `from_date` |
| `store_views` | `/store/storeViews` | 全量扫描 |
| `tax_classes` | `/taxClasses/search` | 全量扫描 |
| `store_groups` | `/store/storeGroups` | 全量扫描 |
| `transactions` | `/transactions` | `created_at` |
| `store_websites` | `/store/websites` | 全量扫描 |
| `customer_groups` | `/customerGroups/search` | 全量扫描 |
| `inventory_stocks` | `/inventory/stocks` | 全量扫描 |
| `inventory_sources` | `/inventory/sources` | 全量扫描 |
| `directory_currency` | `/directory/currency` | 全量扫描 |
| `directory_countries` | `/directory/countries` | 全量扫描 |
| `inventory_source_items` | `/inventory/source-items` | 全量扫描 |

三个店铺端点和国家端点返回不分页的数组，币种端点返回单个对象，其余端点使用从 1 开始的 `searchCriteria` 分页。一个 Source 读取器按配置顺序读取资源。库存资源要求部署已提供 Inventory Management API。

每行包含 `resource STRING`、`id STRING` 和 `record STRING`。最后一列保存完整 JSON 对象，保留嵌套字段、Unicode、空值和数值精度。库存来源项的 ID 是来源代码和 SKU 组成的 JSON 数组，币种对象的 ID 是基础币种代码。

## 配置项

| 配置项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `base_url` | String | 必填 | `/rest` 前的 HTTPS 部署地址，可包含站点目录和端口 |
| `token` | String | 必填 | 具有所选资源读取权限的 Bearer Token |
| `store_code` | String | 空 | 可选的店铺视图代码 |
| `resources` | String 列表 | 21 项全部 | 按扫描顺序列出资源，各名称只可出现一次 |
| `start_time` | String | 可选 | 包含起点的 UTC 时间，格式为 `YYYY-MM-DDTHH:mm:ssZ`；省略时全量扫描 |
| `end_time` | String | Coordinator 启动时间 | 配置 `start_time` 时使用的 UTC 结束时间，不包含该时刻，格式同上 |
| `page_size` | Integer | `100` | 分页资源每次请求的页面大小，1 至 1000 |
| `max_pages` | Integer | `10000` | 每个资源的分页上限，1 至 100000 |
| `max_attempts` | Integer | `4` | 单页最大尝试次数，包含首次请求，1 至 8 |
| `socket_timeout_ms` | Integer | `15000` | 连接和读空闲超时，100 至 120000 毫秒 |
| `page_deadline_ms` | Integer | `60000` | 包含重试、读取和解析在内的单页总时限，500 至 600000 毫秒 |
| `max_response_bytes` | Integer | `8388608` | 响应解压后的字节上限，128 至 33554432 字节 |

时间范围只用于表中带时间字段的资源。销售规则的 `from_date` 是日期字段，按 UTC 零点解释，用来筛选起始日期。若全量扫描，会保留该字段为空的规则。全量资源始终不带时间条件。

对于分页资源，Source 请求稳定顺序，并按 `total_count` 校验页面条数。满足总数的满页和一致的空页都会结束该资源。遇到重复页、总数与条数矛盾或超过页数上限时报告错误。临时故障在页面时限内重试，并遵守有效的 `Retry-After`。HTTP 重定向会触发错误。

检查点保存资源、页号和已输出的行。恢复部分完成的页面时，Source 重新请求并验证页面后跳过已输出的行。恢复时需使用相同的站点地址、店铺视图、资源顺序、页面大小和时间窗口配置。远端集合可能在扫描期间变化。API 没有跨页快照，记录位置可能移动，也可能出现重复。

## 示例

```hocon
env {
  job.mode = "BATCH"
}

source {
  AdobeCommerce {
    base_url = "https://commerce.example.com/shop"
    store_code = "default"
    token = "${commerce_token}"
    resources = ["orders", "customers", "products", "directory_currency"]
    start_time = "2026-01-01T00:00:00Z"
    end_time = "2026-01-02T00:00:00Z"
    page_size = 100
  }
}

sink {
  Console {}
}
```

<ChangeLog />
