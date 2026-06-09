import ChangeLog from '../changelog/connector-http-shopify.md';

# Shopify

> Shopify source connector

## Description

Used to read data from the [Shopify Admin REST API](https://shopify.dev/docs/api/admin-rest). It authenticates with a Shopify Admin API access token (sent in the `X-Shopify-Access-Token` header) and reads a resource endpoint such as orders, products, or customers into SeaTunnel rows.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

|            name             |  type   | required | default value |
|-----------------------------|---------|----------|---------------|
| url                         | String  | Yes      | -             |
| access_token                | String  | Yes      | -             |
| method                      | String  | No       | get           |
| schema                      | Config  | No       | -             |
| format                      | String  | No       | text          |
| params                      | Map     | No       | -             |
| body                        | String  | No       | -             |
| json_field                  | Config  | No       | -             |
| content_field                | String  | No       | -             |
| poll_interval_millis        | int     | No       | -             |
| retry                       | int     | No       | -             |
| retry_backoff_multiplier_ms | int     | No       | 100           |
| retry_backoff_max_ms        | int     | No       | 10000         |
| enable_multi_lines          | boolean | No       | false         |
| common-options              | config  | No       | -             |

### url [String]

The Shopify Admin API endpoint to read from, for example `https://your-store.myshopify.com/admin/api/2024-01/products.json`.

### access_token [String]

The Shopify Admin API access token. It is sent in the `X-Shopify-Access-Token` request header. See the [Shopify authentication docs](https://shopify.dev/docs/api/admin-rest#authentication) for how to obtain one.

### method [String]

http request method, only supports GET, POST method.

### schema [Config]

The structure of the data, including field names and field types. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md).

### format [String]

the format of upstream data, now only support `json` `text`, default `json`.

### params [Map]

http params

### json_field [Config]

This parameter helps you configure the schema, so this parameter must be used with schema. It maps JSON paths in the response to schema fields. See the [Http source](./Http.md) connector for details and examples.

### content_field [String]

This parameter can extract a sub-section of the JSON response (for example the array under a top-level key such as `products` or `orders`) before mapping to rows. See the [Http source](./Http.md) connector for details and examples.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Example

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

## Changelog

<ChangeLog />
