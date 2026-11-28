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

import ChangeLog from '../changelog/connector-http-cbs-statline.md';

# CBS StatLine

> CBS StatLine 源连接器

`CbsStatLine` 源连接器通过荷兰中央统计局（CBS）的 [OData v4 API](https://www.cbs.nl/nl-nl/onze-diensten/open-data/statline-als-open-data/snelstartgids-odata-v4) 读取一个公开 StatLine 数据集的 `Observations` 实体，无需凭据。每行对应一个观测单元格，不是透视后的数据集宽表行。

## 支持的特性

- 有界批量读取，源并行度为 `1`。
- 按服务端返回的 `@odata.nextLink` 翻页，每次运行最多读取 10,000 页；重复翻页链接、重定向或离开当前数据集端点的链接会使任务失败。
- 不支持增量读取、删除捕获、一致性时间点快照或精确一次回放。重跑从第一页开始。下游如需替换数据集，请在目标端选择覆盖或暂存后切换。
- 仅同步观测值。指标和维度的字典需要单独获取。

## 选项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `dataset_id` | String | 是 | — | OData URL 中的 StatLine 数据集标识。 |
| `max_rows` | Integer | 否 | `0` | 正数限制输出行数，并为首次请求设置 OData `$top`；零表示读取可用页，但仍受 10,000 页安全上限约束；超过上限会使任务失败。 |
| `base_url` | String | 否 | `https://datasets.cbs.nl` | 提供相同 API 的源地址。测试网络之外请使用 HTTPS。 |

## 资源限制与恢复

每页实际读取的 HTTP 解压后数据最多为 64 MiB。单个 JSON 字符串或属性名 token 的编码长度最多为 1 MiB（包含引号和转义）。每条观测在创建 JSON 树之前限制为 4 MiB、200,000 个 token；嵌套深度最多 64 层，数字文本最多 1,000 个字符。响应头最多 64 项，每行最多 8,192 个字符；翻页 URL 在 URI 解析前最多为 8,192 个字符。代理 CONNECT 拒绝响应在原生隧道缓冲或清理读取之前限制为 64 KiB。超过固定资源限制会明确失败，不会继续接收任意大小的响应。

连接和 Socket 空闲超时仍为 30 秒。每页另有 120 秒的累计主动请求和解析时限；等待引擎、checkpoint 或下游行限速的时间不计入该时限。HTTP 和解析不占用 collector 的 checkpoint 锁，每接收一行后都会返回引擎的 checkpoint 协作入口。取消任务会中止连接器持有的请求，并阻止迟到数据输出。

观测按流输出，后续响应封装错误可能在已有行输出后导致任务失败。正数 `max_rows` 在接收指定前缀后立即停止，不校验或排空未读取的后缀。Checkpoint 和 savepoint 保存的源状态仍为空：恢复时会重新读取整个数据集，包括之前已输出的行，不保存游标、不提供精确一次输出，也不建立远端快照。

## 输出字段

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `dataset_id` | STRING | 配置的数据集标识。 |
| `id` | BIGINT | 观测 ID，仅在当前数据集内使用。 |
| `measure` | STRING | CBS 指标代码。 |
| `value` | STRING | 数值以文本输出，避免双精度浮点舍入（可能使用指数表示法）；没有数值时为 null。转换目标类型前请确定所需精度。 |
| `string_value` | STRING | CBS 文本值（如有），保留尾部空格。 |
| `value_attribute` | STRING | CBS 数值属性（如有）。 |
| `observation` | STRING | API 观测对象的紧凑 JSON，保留当前数据集的所有维度。 |

## 示例

以下任务读取 CBS OData 入门指南所用公开数据集的两条记录。移除 `max_rows` 即可读取全部记录。

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  CbsStatLine {
    dataset_id = "83765NED"
    max_rows = 2
  }
}

sink {
  Console {}
}
```

HTTP 错误、无效响应或翻页循环会使任务失败，不会被当作读取完成。遇到临时不可用或限流时，可稍后重新运行批任务。

## 变更日志

<ChangeLog />
