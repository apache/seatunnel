import ChangeLog from '../changelog/connector-http-stripe.md';

# Stripe

> Stripe PaymentIntents source connector

## Description

Reads PaymentIntent objects from Stripe as a bounded batch source. Each output row contains one complete PaymentIntent JSON object in the `content` string column.

The connector follows Stripe's reverse-chronological list order and uses the last object ID from each response as the `starting_after` cursor for the next page.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

| name | type | required | default value | description |
|------|------|----------|---------------|-------------|
| secret_key | String | Yes | - | Stripe secret API key. The connector sends it as a Bearer token and does not include it in connector logs. |
| api_base_url | String | No | `https://api.stripe.com` | Stripe API base URL. This is primarily useful for local testing through a compatible HTTP endpoint. |
| api_version | String | No | - | Stripe API version sent in the `Stripe-Version` header. Pin this when a job needs a stable Stripe response contract. |
| page_size | int | No | 100 | PaymentIntents requested per page. The allowed range is 1 to 100. |
| created_gte | long | No | - | Inclusive lower `created` boundary in Unix seconds. |
| created_lt | long | No | - | Exclusive upper `created` boundary in Unix seconds. When both boundaries are set, `created_gte` must be lower than `created_lt`. |
| rate_limit_max_retries | int | No | 3 | Maximum retries after an HTTP 429 response. |
| rate_limit_backoff_ms | int | No | 1000 | Initial exponential backoff after an HTTP 429 response. Backoff is capped at 60 seconds. |
| retry | int | No | - | Maximum retry count for transport-level `IOException` failures. |
| retry_backoff_multiplier_ms | int | No | 100 | Retry backoff multiplier for transport failures. |
| retry_backoff_max_ms | int | No | 10000 | Maximum retry backoff for transport failures. |
| connect_timeout_ms | int | No | 12000 | HTTP connection timeout. |
| socket_timeout_ms | int | No | 60000 | HTTP socket timeout. |
| common-options | config | No | - | Source common options. See [Source Common Options](../common-options/source-common-options.md). |

## Output

The source has a stable single-column schema:

| column | type | description |
|--------|------|-------------|
| content | string | One complete PaymentIntent object serialized as JSON. |

Returning the complete object avoids treating Stripe's expandable fields and metadata keys as a fixed relational schema. Use a transform after this source when individual fields are needed.

PaymentIntent objects can contain sensitive values such as `client_secret`. Protect the source output and downstream storage accordingly. A custom `api_base_url` also receives the configured API key, so use only a trusted HTTPS endpoint outside local testing.

## Time boundaries and recovery

Stripe returns PaymentIntents in reverse chronological order. The connector uses `starting_after` with the last ID from each page, so every request continues toward older objects within the same list operation.

For repeatable scheduled extracts, use a half-open time range: `created_gte` is inclusive and `created_lt` is exclusive. Adjacent runs can therefore use `[previous_end, current_end)` without overlapping their time boundaries.

This V1 connector uses SeaTunnel's bounded single-split source model. If a task fails before the batch completes, recovery starts the bounded read again from the configured time range. Downstream processing should therefore tolerate replayed rows.

The Stripe list API is not a transactional snapshot. A PaymentIntent can change while a multi-page read is in progress. The time range bounds which objects are selected, but it does not freeze their contents. Set `api_version` when the JSON contract must remain stable across account API-version changes.

## Example

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

## Changelog

<ChangeLog />
