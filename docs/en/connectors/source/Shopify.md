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
| headers                     | Map     | No       | -             |
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
| json_filed_missed_return_null | boolean | No     | false         |
| enable_multi_lines          | boolean | No       | false         |
| common-options              | config  | No       | -             |

`pageing` appears in the option rule but is rejected at startup by this connector — see [Pagination](#pagination).

### url [String]

The Shopify Admin API endpoint to read from, for example `https://your-store.myshopify.com/admin/api/2024-01/products.json`.

### access_token [String]

The Shopify Admin API access token. It is sent in the `X-Shopify-Access-Token` request header. See the [Shopify authentication docs](https://shopify.dev/docs/api/admin-rest#authentication) for how to obtain one.

### method [String]

http request method, only supports GET, POST method.

### headers [Map]

Extra HTTP request headers. The connector already sets `X-Shopify-Access-Token` from
`access_token` and `Accept: application/json`, so this is only for anything beyond those —
setting `X-Shopify-Access-Token` here is overwritten by `access_token`.

### schema [Config]

The structure of the data, including field names and field types. For more details, please refer to [Schema Feature](../../introduction/concepts/schema-feature.md).

### format [String]

the format of upstream data, now only support `json` `text`, default `text`.

### params [Map]

http params

### json_field [Config]

This parameter helps you configure the schema, so this parameter must be used with schema. It maps JSON paths in the response to schema fields. See the [Http source](./Http.md) connector for details and examples.

### content_field [String]

This parameter can extract a sub-section of the JSON response (for example the array under a top-level key such as `products` or `orders`) before mapping to rows. See the [Http source](./Http.md) connector for details and examples.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Pagination

**Not supported.** `pageing` is inherited from the HTTP source option rule, but this connector
does not pass it to the reader, so honouring it would read only the first response while the job
reported success. Setting it therefore fails at startup with `HTTP-03`, rather than silently
returning partial data.

Wiring the inherited pagination through would not help on its own: the shared implementation
reads the next cursor out of the response *body* with a JsonPath, while the Admin REST API
returns it in the `Link` response header. Supporting it properly means teaching
`connector-http-base` to read a header cursor.

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

`${SHOPIFY_ACCESS_TOKEN}` is a SeaTunnel config variable, not an environment variable — it is
substituted only when the value is supplied on the command line:

```bash
./bin/seatunnel.sh -c your_app.conf -i SHOPIFY_ACCESS_TOKEN=shpat_xxx
```

Without `-i`, the literal text `${SHOPIFY_ACCESS_TOKEN}` is sent as the token and Shopify answers
`401`. See [variable configuration](../../introduction/concepts/config.md).


## Changelog

<ChangeLog />
