import ChangeLog from '../changelog/connector-http-stripe.md';

# Stripe

> Stripe PaymentIntents 源连接器

## 描述

以有界批处理方式读取 Stripe PaymentIntent 对象。每一行输出都在 `content` 字符串列中包含一个完整的 PaymentIntent JSON 对象。

连接器按照 Stripe 的逆时间顺序读取列表，并使用每次响应中最后一个对象的 ID 作为下一页请求的 `starting_after` 游标。

## 关键特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [ ] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户定义分片](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称 | 类型 | 是否必填 | 默认值 | 说明 |
|------|------|----------|--------|------|
| secret_key | String | 是 | - | Stripe 私有 API Key。连接器以 Bearer Token 发送该值，不会把它写入连接器日志。 |
| api_base_url | String | 否 | `https://api.stripe.com` | Stripe API 基础地址，主要用于通过兼容的 HTTP 地址进行本地测试。 |
| api_version | String | 否 | - | 通过 `Stripe-Version` 请求头指定 Stripe API 版本。需要稳定响应契约时建议固定该值。 |
| page_size | int | 否 | 100 | 每页请求的 PaymentIntent 数量，范围为 1 到 100。 |
| created_gte | long | 否 | - | `created` 时间的包含式下界，使用 Unix 秒。 |
| created_lt | long | 否 | - | `created` 时间的不包含式上界，使用 Unix 秒。两个边界同时配置时，`created_gte` 必须小于 `created_lt`。 |
| rate_limit_max_retries | int | 否 | 3 | 收到 HTTP 429 后的最大重试次数。 |
| rate_limit_backoff_ms | int | 否 | 1000 | 收到 HTTP 429 后的初始指数退避时间，最大为 60 秒。 |
| retry | int | 否 | - | 传输层发生 `IOException` 时的最大重试次数。 |
| retry_backoff_multiplier_ms | int | 否 | 100 | 传输失败时的重试退避倍数。 |
| retry_backoff_max_ms | int | 否 | 10000 | 传输失败时的最大重试退避时间。 |
| connect_timeout_ms | int | 否 | 12000 | HTTP 连接超时时间。 |
| socket_timeout_ms | int | 否 | 60000 | HTTP Socket 超时时间。 |
| common-options | config | 否 | - | Source 通用选项，详见 [Source Common Options](../common-options/source-common-options.md)。 |

## 输出

该 Source 使用固定的单列结构：

| 列名 | 类型 | 说明 |
|------|------|------|
| content | string | 序列化为 JSON 的完整 PaymentIntent 对象。 |

完整返回对象可以避免把 Stripe 可展开字段和动态 metadata 键误认为固定的关系型结构。需要单独字段时，可以在 Source 后增加 Transform。

PaymentIntent 对象可能包含 `client_secret` 等敏感值，请保护 Source 输出和下游存储。自定义 `api_base_url` 也会收到配置的 API Key，因此除本地测试外，只应使用可信的 HTTPS 地址。

## 时间边界和恢复

Stripe 按逆时间顺序返回 PaymentIntent。连接器使用每一页最后一个 ID 作为 `starting_after`，使后续请求继续读取更早的对象。

对于可重复执行的定时抽取，建议使用半开时间范围：`created_gte` 包含下界，`created_lt` 不包含上界。相邻任务可以使用 `[previous_end, current_end)`，不会在时间边界上产生重叠。

V1 使用 SeaTunnel 有界单分片 Source 模型。如果任务在批处理完成前失败，恢复时会从配置的时间范围重新开始读取。因此下游处理应能接受重复读取的行。

Stripe 列表 API 不是事务快照。多页读取期间，PaymentIntent 的内容可能发生变化。时间范围只限制被选择的对象，不会冻结对象内容。需要避免账户 API 版本变化影响 JSON 契约时，请配置 `api_version`。

## 示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Stripe {
    plugin_output = "stripe_payment_intents"
    secret_key = "${STRIPE_SECRET_KEY}"
    api_version = "2026-02-25.clover"
    page_size = 100
    created_gte = 1754006400
    created_lt = 1754092800
  }
}

sink {
  Console {
    plugin_input = "stripe_payment_intents"
  }
}
```

## 变更日志

<ChangeLog />
