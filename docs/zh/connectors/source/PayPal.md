# PayPal

> PayPal Transaction Search 数据源连接器。

## 描述

使用第一方 OAuth 客户端凭据，通过 `GET /v1/reporting/transactions` 读取一个账户的有界交易报表。仅支持 BATCH，不是 CDC、支付接口或余额对账服务。

## 主要功能

- [x] 批处理
- [ ] 流处理
- [ ] Exactly-once

并行度必须为 1。恢复时重新读取整个配置窗口，不保存分页偏移量。任务失败前可能已向非事务型下游写入部分记录。

## 前提条件

REST 应用必须获得其所属账户的 Transaction Search 权限。请启用应用的交易查询权限并确认报表访问；获取 OAuth token 成功不代表具有报表权限。权限变更可能需要重新获取 token。不支持第三方账户或合作伙伴授权流程。

生产地址为 `https://api-m.paypal.com`，沙箱地址为 `https://api-m.sandbox.paypal.com`。沙箱凭据和报表可用性独立于生产环境。模拟测试不证明沙箱或生产权限、数据可用性或完整性。本连接器不声称已通过真实账户验证。

参考 [官方 OpenAPI](https://github.com/paypal/paypal-rest-api-specifications/blob/main/openapi/reporting_transactions_v1.json)、[交易查询](https://developer.paypal.com/api/transaction-search/v1/search-get) 和 [REST 认证](https://developer.paypal.com/api/rest/authentication/)。

## 报表约定

必须指定带秒和时区偏移的绝对 RFC3339 `start_date`、`end_date`。开始早于结束，跨度不超过 31 天。时间统一转换为 UTC，不使用主机时区。PayPal 提供最近三年的历史数据，交易最多可能延迟三小时才出现在报表中。

固定请求 `fields=all` 和 `balance_affecting_records_only=N`，保留影响余额及不影响余额的所有返回记录。交易 ID 在报表系统中不唯一，不建立主键、不按 ID 去重，下游也应保留此区别。

每页必须包含账户、时间范围、页码、总数及交易数组。返回时间范围经 UTC 归一化后必须与请求完全一致。PayPal 的 `end_date` 也可能表示当前可提供数据的最后时间：若返回范围缩短，任务失败，不会静默修改请求。请等待报表可用，或显式缩小窗口后重跑。

遇到格式错误、HTTP 200 中的业务错误、分页数量不一致、账户或总数变化及可检测的截断时失败。`RESULTSET_TOO_LARGE` 提示缩小日期窗口。连接器限制为最多 10,000 条；恰好 10,000 条且所有分页及总数校验通过时可读取，超过限制则失败。不截断、不自动拆分窗口。调整窗口时须明确核对边界和重放影响。

总数稳定、时间范围匹配只能检测部分问题，不代表远端不可变快照。同数量的记录变化、延迟更新或遗漏可能无法检测。不保证快照一致性、无损时间切分或完整历史。

## 输出结构

固定结构，不支持自定义 `schema`。

| 字段 | 类型 | 可空 | 含义 |
| --- | --- | --- | --- |
| account_number | STRING | 否 | 响应中的报表账户 |
| transaction_id | STRING | 是 | 非唯一交易 ID |
| transaction_event_code | STRING | 是 | 事件代码 |
| transaction_status | STRING | 是 | 状态 |
| transaction_initiation_date | STRING | 是 | UTC RFC3339 创建时间 |
| transaction_updated_date | STRING | 是 | UTC RFC3339 更新时间 |
| transaction_amount | DECIMAL(38,9) | 是 | 原币单位总金额 |
| transaction_currency | STRING | 是 | 总金额币种 |
| fee_amount | DECIMAL(38,9) | 是 | 原币单位手续费 |
| fee_currency | STRING | 是 | 手续费币种，与总金额币种独立 |
| content | STRING | 否 | 完整交易对象 JSON，包含引用及额外字段 |

可选字段缺失或为 null 均保留为空。存在的金额对象必须含字符串金额和三字母币种；支持零位及三位小数币种。金额须能精确表示为 DECIMAL(38,9)，超出精度或小数位时失败而非舍入。其他金额字段保留在原始 JSON 中，不进行浮点转换。原始记录可能包含个人或财务数据，请使用受控下游并避免记录到日志。

## 配置项

| 名称 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| client_id | STRING | 是 | - | 第一方 REST 应用 ID |
| client_secret | STRING | 是 | - | 客户端密钥，使用原生配置日志脱敏 |
| start_date | STRING | 是 | - | 带秒和偏移的绝对开始时间 |
| end_date | STRING | 是 | - | 绝对结束时间，窗口不超过 31 天 |
| api_base_url | STRING | 否 | https://api-m.paypal.com | 精确生产或沙箱源地址，不带末尾斜线 |
| page_size | INT | 否 | 100 | 1 到 500 |
| max_retries | INT | 否 | 3 | 额外瞬时故障重试，0 到 5 |
| retry_delay_ms | INT | 否 | 1000 | 1 到 60000 毫秒 |
| request_timeout_ms | INT | 否 | 30000 | 连接、读取及请求中止期限，1 到 120000 毫秒 |
| max_response_bytes | INT | 否 | 8388608 | 每次响应解压后字节上限，1024 到 16777216 |
| mock_mode | BOOLEAN | 否 | false | 测试自定义源地址，凭据必须严格为 mock-client / mock-secret |

通过真实 token 接口获取 OAuth token，到期或每页首次 HTTP 401 后刷新。仅对 HTTP 429/500/502/503/504 及传输错误进行有界重试。遵守最多 60 秒的数字 Retry-After；更长或不支持的值直接失败，请稍后重跑。不跟随重定向。关闭时中止活动请求并唤醒重试等待。

请求期限不能中断 JVM DNS 解析或阻塞的下游 collect，因此不是无条件的任务总耗时保证。分页数、响应大小、请求次数和等待时间均受限。请关闭 HTTP wire/header 日志；密钥不得写入 URL、点分配置名或诊断信息。使用环境变量或认可的密钥提供方式。模拟模式不是官方 PayPal 模拟器，不得使用真实凭据。

## 示例

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
  PayPal {
    plugin_output = "transactions"
    client_id = ${PAYPAL_CLIENT_ID}
    client_secret = ${PAYPAL_CLIENT_SECRET}
    start_date = "2026-01-01T00:00:00Z"
    end_date = "2026-01-02T00:00:00Z"
    page_size = 100
  }
}
sink {
  LocalFile {
    plugin_input = "transactions"
    path = "/data/paypal"
    file_format_type = "json"
  }
}
```

## 变更日志

[变更日志](../changelog/connector-http-paypal.md)
