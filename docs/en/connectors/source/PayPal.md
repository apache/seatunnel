import ChangeLog from '../changelog/connector-http-paypal.md';

# PayPal

> PayPal Transaction Search source connector.

## Description

Reads one account's transactions through `GET /v1/reporting/transactions` using first-party OAuth client credentials. This is a bounded BATCH report, not CDC, a payment API, or a balance reconciliation service.

## Key Features

- [x] Batch
- [ ] Streaming
- [ ] Exactly-once

Parallelism must be 1. Recovery replays the entire configured window; no page offset is checkpointed. A failed job may already have emitted rows to a nontransactional sink.

## Prerequisites

Use a PayPal REST application authorized for Transaction Search for its own account. Enable Transaction Search permissions for the app and verify the issued credentials have reporting access; successful token issuance alone does not prove this. Permission changes may require a fresh token. Third-party account access and partner authorization are out of scope.

Production origin is `https://api-m.paypal.com`; sandbox origin is `https://api-m.sandbox.paypal.com`. Sandbox credentials and reporting availability are separate from production. A mock test does not prove sandbox or production permissions, data availability, or completeness. This connector has no live-account verification claim.

See [the official Transaction Search schema](https://github.com/paypal/paypal-rest-api-specifications/blob/main/openapi/reporting_transactions_v1.json), [List transactions](https://developer.paypal.com/api/transaction-search/v1/search-get), and [REST authentication](https://developer.paypal.com/api/rest/authentication/).

## Reporting Contract

Supply explicit absolute RFC3339 `start_date` and `end_date` with seconds and an offset, with start before end and at most 31 days between them. Dates are normalized to UTC, never the host time zone. PayPal provides up to three years of history and transactions can take up to three hours to appear.

The connector always sends `fields=all` and `balance_affecting_records_only=N`, retaining both balance-affecting and non-affecting reporting records. Transaction IDs are NOT unique; no primary key or ID deduplication is applied. Preserve this distinction in downstream storage.

Every response must include account, dates, page, totals, and a transaction array. Returned dates must cover exactly the requested interval after UTC normalization. In particular, PayPal's returned `end_date` may instead be the last date available; a shortened response fails. Wait for reporting availability or explicitly narrow the request, then rerun. The connector does not silently move the requested end.

Malformed rows, API error envelopes (even with HTTP 200), inconsistent page sizes, changed account/totals, and detected truncation fail the job. `RESULTSET_TOO_LARGE` fails with advice to narrow the date window. The connector safety cap is 10,000 records; exactly 10,000 is accepted when every page and count validates. Larger results fail. The connector never truncates or automatically splits the interval. Reconcile boundaries and replay effects explicitly when changing windows.

Stable totals and matching coverage detect only some problems. PayPal does not provide a remote immutable snapshot here; same-count changes, delayed updates, or omissions may remain undetectable. No snapshot-consistency, lossless interval-splitting, or complete-history guarantee is made.

## Output Schema

The schema is fixed; no user-defined `schema` option is supported.

| Field | Type | Nullable | Meaning |
| --- | --- | --- | --- |
| account_number | STRING | no | Reporting account from the response |
| transaction_id | STRING | yes | Non-unique reporting transaction ID |
| transaction_event_code | STRING | yes | PayPal event code |
| transaction_status | STRING | yes | PayPal status |
| transaction_initiation_date | STRING | yes | RFC3339 timestamp normalized to UTC |
| transaction_updated_date | STRING | yes | RFC3339 timestamp normalized to UTC |
| transaction_amount | DECIMAL(38,9) | yes | Gross amount in native currency units |
| transaction_currency | STRING | yes | Gross amount currency |
| fee_amount | DECIMAL(38,9) | yes | Fee in native currency units |
| fee_currency | STRING | yes | Fee currency, independent of gross currency |
| content | STRING | no | Entire transaction detail object as JSON, including references and additional fields |

Optional fields may be absent or null. Present money objects must contain a string amount and three-letter currency. Zero- and three-decimal currencies are supported. Amounts must fit DECIMAL(38,9) exactly; excess scale or precision fails rather than rounding. Additional monetary fields remain in `content` in their original JSON representation. No floating-point conversion is used. Raw records can contain personal or financial data: use access-controlled sinks and do not log them.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| client_id | STRING | yes | - | First-party REST app client ID |
| client_secret | STRING | yes | - | Client secret; native parsed-config log masking applies |
| start_date | STRING | yes | - | Absolute start with seconds and offset |
| end_date | STRING | yes | - | Absolute end, no more than 31 days after start |
| api_base_url | STRING | no | https://api-m.paypal.com | Exact production or sandbox origin, no trailing slash |
| page_size | INT | no | 100 | 1 to 500 |
| max_retries | INT | no | 3 | Additional transient retries, 0 to 5 |
| retry_delay_ms | INT | no | 1000 | 1 to 60000 |
| request_timeout_ms | INT | no | 30000 | Connect/read timeout and request abort deadline, 1 to 120000 |
| max_response_bytes | INT | no | 8388608 | Decompressed bytes per response, 1024 to 16777216 |
| mock_mode | BOOLEAN | no | false | Custom origin for tests; requires exactly mock-client / mock-secret |

OAuth tokens are acquired via the real token endpoint and refreshed on expiry or once after HTTP 401 per page. HTTP 429/500/502/503/504 and transport failures use bounded retries. Other errors fail. Numeric Retry-After up to 60 seconds is honored; longer or unsupported values fail so the job can be retried later. Redirects are not followed. Close aborts active HTTP requests and wakes retry waits.

Request deadlines cannot interrupt JVM DNS resolution or a blocked downstream collector; they are not an unconditional wall-clock job deadline. The page count, response size, attempt count and wait duration are bounded. Keep HTTP wire/header logging disabled. Do not place secrets in endpoint URLs, dotted option names, or diagnostics. Use environment substitution or an approved secret provider. `mock_mode` is not an official PayPal emulator and must never use real credentials.

## Example

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

## Changelog

<ChangeLog />
