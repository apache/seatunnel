import ChangeLog from '../changelog/connector-http-intercom.md';

# Http-Intercom

> Intercom 源连接器

## 描述

通过 Intercom REST API（`GET /contacts`）读取联系人。基于 `connector-http-base` 构建，仅支持批处理模式。
第一版只读取联系人，不支持会话、工单、公司、搜索、OAuth 和自定义属性。

## 主要特性

- [x] 批处理
- [ ] 流处理
- [ ] 精确一次
- [ ] 列投影
- [ ] 并行度

## 选项

| 名称 | 类型 | 必填 | 默认值 | 描述 |
| --- | --- | --- | --- | --- |
| api_key | String | 是 | - | Intercom 访问令牌（原始令牌，不带 `Bearer ` 前缀）。 |
| base_url | String | 否 | `https://api.intercom.io` | EU 或 AU 区域的工作区请使用 `https://api.eu.intercom.io` 或 `https://api.au.intercom.io`。 |
| api_version | String | 否 | `2.17` | 作为 `Intercom-Version` 请求头发送，避免工作区默认版本变化导致响应结构改变。 |
| per_page | Int | 否 | `150` | 每页联系人数量，范围 1 到 150。 |
| retry | Int | 否 | `3` | HTTP 429 和 5xx 的最大重试次数。 |
| retry_backoff_multiplier_ms | Int | 否 | `100` | 退避倍数（毫秒）。 |
| retry_backoff_max_ms | Int | 否 | `10000` | 最大退避时间（毫秒）。 |

## 输出列

`id`、`role`、`email`、`name`、`phone`、`external_id`（均为 STRING，后三者可为 null），
以及 `created_at`、`updated_at`（BIGINT，Unix 秒级时间戳）。

## 示例

```hocon
env {
  execution.parallelism = 1
  job.mode = BATCH
}

source {
  Intercom {
    api_key = "<ACCESS_TOKEN>"
    plugin_output = "intercom_contacts"
  }
}

sink {
  Console {
    plugin_input = "intercom_contacts"
  }
}
```

## 注意事项

每次运行都是联系人的有界快照，从第一页开始沿 `pages.next.starting_after` 游标读取。列表接口没有“按更新时间过滤”，因此没有增量模式。
作业失败后重启会从头读取，可能重复发送已发送的行，读取期间新建或合并的联系人也可能在分页之间出现或消失。
每行都包含联系人 `id`，下游可据此去重或 upsert，但连接器不提供投递保证。

<ChangeLog />
