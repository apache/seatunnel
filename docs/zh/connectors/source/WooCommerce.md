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

import ChangeLog from '../changelog/connector-http-woocommerce.md';

# WooCommerce

> 从一个 WooCommerce 商店读取订单。

## 描述

通过 HTTPS 调用 `GET /wp-json/wc/v3/orders`，批量读取指定 UTC 创建时间范围内的订单。仅支持 BATCH 和 parallelism=1，不支持 CDC、流式读取、Exactly-once 或事务一致性快照。连接器只发送 GET，不修改订单。

## 使用前提

为有订单读取权限的用户创建 Read 权限 REST API 密钥。配置最终 HTTPS 商店地址（可包含 WordPress 子目录）；不跟随重定向。服务器证书必须通过 JVM 校验，私有 CA 应导入每个工作节点的 JVM truststore。WordPress 需要向 WooCommerce 转发 Authorization 头。连接器使用 consumer key/secret 的 Basic 认证，不使用 WordPress 登录密码或公开 Store API。

使用环境变量或密钥服务配置凭据。解析配置日志会隐藏 consumer_key 和 consumer_secret，但必须禁用 HTTP wire/header 日志，且不得将密钥放入 URL。

## 读取约定

start_date 与 end_date 均为排他的订单创建时间边界，必须包含秒和时区偏移；连接器转为 UTC 并发送 dates_are_gmt=true。拼接相邻窗口可能遗漏恰好位于边界的订单；需要组合导出时请重叠窗口并按商店及订单 ID 去重。创建时间过滤不能捕获后续修改和删除。

请求按 ID 升序分页，从第 1 页开始。每页必须包含 X-WP-Total 和 X-WP-TotalPages 及匹配数量的订单。缺失分页头、总数变化、重复或倒序 ID、无效响应及超过 max_pages 都会失败，而不是静默截断。空窗口产生零行。

恢复时重新读取整个窗口；分页位置不是持久化快照。失败前可能已有部分数据写入非事务型 sink，应使用幂等写入。稳定的总数和递增 ID 不能检测所有并发修改，因此不承诺快照完整性。

## 参数

| 参数 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| url | STRING | 是 | - | HTTPS 商店地址，可包含安装子目录，不能带凭据、查询或片段 |
| consumer_key | STRING | 是 | - | Read 权限 REST API key |
| consumer_secret | STRING | 是 | - | REST API secret |
| start_date | STRING | 是 | - | 排他的 UTC 创建时间下界 |
| end_date | STRING | 是 | - | 排他的 UTC 创建时间上界，必须晚于下界 |
| schema | CONFIG | 是 | - | 选择的订单字段及 SeaTunnel 类型 |
| page_size | INT | 否 | 100 | 每页数量，1 至 100 |
| max_pages | INT | 否 | 10000 | 页数上限，1 至 1000000；超过时在输出第一页前失败 |
| decimal_places | INT | 否 | 2 | WooCommerce dp 参数，0 至 18 |
| max_retries | INT | 否 | 3 | 每页额外重试次数，0 至 5 |
| retry_delay_ms | INT | 否 | 1000 | 最小重试等待，1 至 60000 毫秒 |
| request_timeout_ms | INT | 否 | 30000 | 单次请求超时及中止期限，1 至 120000 毫秒 |
| max_response_bytes | INT | 否 | 8388608 | 每页解压后的字节上限，1024 至 16777216 |

支持引擎默认 FAIL_FAST 策略，不支持跳过表策略。HTTP 429/500/502/503/504 和传输错误会有界重试，其余状态失败。支持最长 60 秒的数值或 HTTP 日期 Retry-After，超出或无效则失败。关闭 reader 会中止请求并唤醒等待。该期限针对单次 HTTP I/O，不保证整个作业的严格时长；DNS 解析、JSON 解析、重试等待及下游阻塞不在此保证范围内。

## 数据类型

复用 SeaTunnel JSON schema 映射，支持嵌套 ROW、MAP 和已有 ARRAY 类型。缺失或 null 字段输出 null，未选择字段忽略。金额应选择 DECIMAL；返回金额若不能精确适配声明的精度/小数位则失败，不会舍入。但 WooCommerce 会先按 decimal_places 格式化金额，默认 2 不能保留商店中更高精度的值，需按业务配置。

line_items 等嵌套数组可用 STRING 保留 JSON，或用 array<map<string,string>>；当前公共 schema 解析器不支持 ARRAY&lt;ROW&gt;。订单可能包含个人信息，应使用有访问控制的 sink，不记录输出行。

## 示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
  WooCommerce {
    url = "https://shop.example.com"
    consumer_key = ${WOOCOMMERCE_CONSUMER_KEY}
    consumer_secret = ${WOOCOMMERCE_CONSUMER_SECRET}
    start_date = "2026-01-01T00:00:00Z"
    end_date = "2026-02-01T00:00:00Z"
    decimal_places = 3
    schema {
      fields {
        id = bigint
        total = "decimal(18,3)"
        billing = { country = string }
        line_items = "array<map<string,string>>"
      }
    }
  }
}
sink {
  LocalFile {
    path = "/data/orders"
    file_format_type = "json"
  }
}
```

参考 [WooCommerce 认证](https://developer.woocommerce.com/docs/apis/rest-api/authentication/) 与 [订单 API](https://developer.woocommerce.com/docs/apis/rest-api/v3/orders/)。

## 变更记录

<ChangeLog />
