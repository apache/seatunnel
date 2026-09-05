import ChangeLog from '../changelog/connector-http-shopify.md';

# Shopify

> Shopify 数据源连接器

## 描述

用于从 [Shopify Admin REST API](https://shopify.dev/docs/api/admin-rest) 读取数据。它使用 Shopify Admin API 的访问令牌进行认证（通过 `X-Shopify-Access-Token` 请求头发送），并将某个资源接口（如 orders、products、customers）读取为 SeaTunnel 的行数据。

## 主要特性

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [ ] [并行度](../../introduction/concepts/connector-v2-features.md)
- [ ] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)

## 选项

|            名称             |  类型   | 是否必填 |   默认值   |
|-----------------------------|---------|----------|-----------|
| url                         | String  | 是       | -         |
| access_token                | String  | 是       | -         |
| method                      | String  | 否       | get       |
| headers                     | Map     | 否       | -         |
| schema                      | Config  | 否       | -         |
| format                      | String  | 否       | text      |
| params                      | Map     | 否       | -         |
| body                        | String  | 否       | -         |
| json_field                  | Config  | 否       | -         |
| content_field                | String  | 否       | -         |
| poll_interval_millis        | int     | 否       | -         |
| retry                       | int     | 否       | -         |
| retry_backoff_multiplier_ms | int     | 否       | 100       |
| retry_backoff_max_ms        | int     | 否       | 10000     |
| json_filed_missed_return_null | boolean | 否     | false     |
| enable_multi_lines          | boolean | 否       | false     |
| common-options              | config  | 否       | -         |

`pageing` 出现在选项规则中，但本连接器会在启动时拒绝它 —— 见[分页](#分页)。

### url [String]

要读取的 Shopify Admin API 接口地址，例如 `https://your-store.myshopify.com/admin/api/2024-01/products.json`。

### access_token [String]

Shopify Admin API 的访问令牌，通过 `X-Shopify-Access-Token` 请求头发送。获取方式请参考 [Shopify 认证文档](https://shopify.dev/docs/api/admin-rest#authentication)。

### method [String]

http 请求方法，仅支持 GET、POST 方法。

### headers [Map]

额外的 HTTP 请求头。连接器已经会根据 `access_token` 设置 `X-Shopify-Access-Token`，并设置
`Accept: application/json`，因此这里只需要配置这两者之外的请求头；在这里设置
`X-Shopify-Access-Token` 会被 `access_token` 覆盖。

### schema [Config]

数据的结构，包括字段名称和字段类型。更多详情请参考 [Schema Feature](../../introduction/concepts/schema-feature.md)。

### format [String]

上游数据的格式，目前仅支持 `json` 和 `text`，默认 `text`。

### params [Map]

http 请求参数。

### json_field [Config]

该参数用于配置 schema，因此必须与 schema 一起使用。它将响应中的 JSON 路径映射到 schema 字段。详情和示例请参考 [Http source](./Http.md) 连接器。

### content_field [String]

该参数可以在映射为行之前，提取 JSON 响应中的某个子部分（例如顶层键 `products` 或 `orders` 下的数组）。详情和示例请参考 [Http source](./Http.md) 连接器。

### common options

数据源插件通用参数，详情请参考 [Source Common Options](../common-options/source-common-options.md)。

## 分页

**不支持。** `pageing` 继承自 HTTP source 的选项规则，但本连接器不会把它传给 reader；若照单
接受，作业只会读取第一次响应的数据却仍报告成功。因此配置该选项会在启动时以 `HTTP-03` 失败，
而不是静默返回不完整的数据。

仅仅把继承来的分页参数传下去也解决不了问题：共享实现是用 JsonPath 从响应*体*中读取下一个游标，
而 Admin REST API 把游标放在 `Link` 响应头里。要真正支持，需要让 `connector-http-base`
能够从响应头读取游标。

## 示例

```hocon
source {
  Shopify {
    url = "https://your-store.myshopify.com/admin/api/2024-01/products.json"
    access_token = "${SHOPIFY_ACCESS_TOKEN}"
    method = "GET"
    format = "json"
    content_field = "$.products.*"
    schema = {
      fields {
        id = string
        title = string
        vendor = string
        product_type = string
        created_at = string
        updated_at = string
      }
    }
  }
}
```

`${SHOPIFY_ACCESS_TOKEN}` 是 SeaTunnel 的配置变量，不是环境变量 —— 只有在命令行传入对应的值时才会被替换：

```bash
./bin/seatunnel.sh -c your_app.conf -i SHOPIFY_ACCESS_TOKEN=shpat_xxx
```

若不传 `-i`，字面量 `${SHOPIFY_ACCESS_TOKEN}` 会被当作令牌发送，Shopify 将返回 `401`。参见[变量配置](../../introduction/concepts/config.md)。


## 变更日志

<ChangeLog />
