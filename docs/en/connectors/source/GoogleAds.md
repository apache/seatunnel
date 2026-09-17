# GoogleAds

> Google Ads source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

## Description

Reads data from Google Ads resources (campaign, ad_group, keyword_view, ...)
using the Google Ads REST API (`googleAds:search`) with GAQL queries. Supports
single-resource, full-GAQL-query, and multi-table (`tables_configs`) batch
ingestion. Results are streamed page by page via `nextPageToken`, so only one
page is held in memory at a time.

Authentication uses the OAuth 2.0 refresh-token grant plus a Google Ads
developer token. The access token is refreshed automatically when it expires.

The output schema is derived automatically: the connector queries the
`googleAdsFields:search` metadata service for the data type of every selected
field and builds the schema **in the SELECT field order**, so column `i`
always corresponds to selected field `i`.

## Supported DataSource Info

| Datasource | Supported Versions   |
|------------|----------------------|
| Google Ads | REST API v21 (default, configurable via `api_version`) |

## Prerequisites

1. A Google Ads **developer token** (from the API Center of a manager/MCC account).
2. An OAuth2 **client ID / client secret** (Google Cloud Console, OAuth consent
   configured for the `https://www.googleapis.com/auth/adwords` scope).
3. A **refresh token** authorized for that scope (e.g. via the OAuth 2.0
   Playground or the Google Ads API oauth helper scripts).
4. The **customer ID** of the account to query (digits only, no dashes). If it
   is a client account under an MCC, also set `login_customer_id` to the MCC
   customer ID.

## Source Options

| Name               | Type    | Required | Default | Description                                                                                          |
|--------------------|---------|----------|---------|------------------------------------------------------------------------------------------------------|
| developer_token    | String  | Yes      | -       | Google Ads API developer token.                                                                       |
| client_id          | String  | Yes      | -       | OAuth2 client ID.                                                                                     |
| client_secret      | String  | Yes      | -       | OAuth2 client secret.                                                                                 |
| refresh_token      | String  | Yes      | -       | OAuth2 refresh token for the `adwords` scope.                                                         |
| customer_id        | String  | Yes      | -       | Customer ID to query, digits only, e.g. `1234567890`.                                                 |
| login_customer_id  | String  | No       | -       | Manager (MCC) customer ID, digits only. Required when `customer_id` is managed by an MCC.             |
| api_version        | String  | No       | v21     | Google Ads REST API version.                                                                          |
| resource           | String  | No*      | -       | Resource for single-table mode, e.g. `campaign`. Requires `fields`. Exclusive with `query` and `tables_configs`. |
| fields             | List    | No       | -       | Ordered list of GAQL field paths, e.g. `[campaign.id, metrics.clicks]`. The output schema follows this order. |
| filter             | String  | No       | -       | GAQL WHERE clause appended to the auto-built `SELECT <fields> FROM <resource>` query.                 |
| query              | String  | No*      | -       | Full GAQL query. Exclusive with `resource`/`fields`/`filter` and `tables_configs`.                    |
| tables_configs     | List    | No*      | -       | Multi-table configuration list. Each entry requires `table_path`. Exclusive with `resource` and `query`. |
| request_timeout_ms | Integer | No       | 60000   | HTTP request timeout in milliseconds for a single call.                                               |
| max_retries        | Integer | No       | 3       | Maximum retries for transient HTTP failures (429/5xx/network errors) of a single request.             |
| retry_backoff_ms   | Long    | No       | 1000    | Base backoff in milliseconds between retries; doubled per attempt.                                    |
| page_size          | Integer | No       | -       | Page size for search requests. When unset the server default is used. Note: recent API versions ignore or reject an explicit page size. |

\* Exactly one of `resource`, `query` or `tables_configs` must be provided.

GAQL has no `SELECT *`, so the field list is always explicit — either via
`fields` or inside `query`. Invalid field names are rejected before any data is
read, with the offending name in the error message. Invalid GAQL (HTTP 400) is
never retried and the API error message is surfaced as-is.

### tables_configs entry options

| Name        | Type   | Required | Description                                                                    |
|-------------|--------|----------|--------------------------------------------------------------------------------|
| table_path  | String | Yes      | Format: `database.resource`, e.g. `google_ads.campaign`.                       |
| fields      | List   | No*      | Ordered GAQL field paths for this table.                                        |
| query       | String | No*      | Full GAQL query for this table. Its `FROM` resource must match `table_path`.    |
| filter      | String | No       | GAQL WHERE clause (only with `fields`).                                         |
| customer_id | String | No       | Per-table customer ID override; falls back to the global `customer_id`.         |

\* Exactly one of `fields` or `query` per entry.

## Data Type Mapping

| Google Ads Data Type       | SeaTunnel Data Type | Notes                                                                    |
|----------------------------|---------------------|--------------------------------------------------------------------------|
| INT64, UINT64              | BIGINT              | The REST API serializes int64 as a JSON string; parsed to long.          |
| INT32                      | INT                 |                                                                          |
| DOUBLE, FLOAT              | DOUBLE              |                                                                          |
| BOOLEAN                    | BOOLEAN             |                                                                          |
| DATE                       | STRING              | Deliberate: date-like fields have non-uniform formats (`2026-09-01`, `2026-09`, `2026-36`). |
| STRING, ENUM, RESOURCE_NAME | STRING             | Enums are symbolic strings, e.g. `ENABLED`.                              |
| MESSAGE                    | STRING              | Nested messages are emitted as JSON text.                                |
| Other / unknown            | STRING              | Forward-compatible fallback.                                             |

Fields absent from a result row (the API omits empty fields entirely) are
emitted as `null`.

## Example

### Single resource

```hocon
source {
  GoogleAds {
    developer_token = "your_developer_token"
    client_id       = "your_client_id"
    client_secret   = "your_client_secret"
    refresh_token   = "your_refresh_token"
    customer_id     = "1234567890"

    resource = "campaign"
    fields   = ["campaign.id", "campaign.name", "campaign.status", "metrics.clicks", "metrics.impressions"]
    filter   = "segments.date DURING LAST_30_DAYS"
  }
}
```

### Full GAQL query

```hocon
source {
  GoogleAds {
    developer_token = "your_developer_token"
    client_id       = "your_client_id"
    client_secret   = "your_client_secret"
    refresh_token   = "your_refresh_token"
    customer_id     = "1234567890"

    query = "SELECT ad_group.id, ad_group.name, metrics.clicks FROM ad_group WHERE metrics.clicks > 0"
  }
}
```

### Multiple tables (with per-table customer ID)

```hocon
source {
  GoogleAds {
    developer_token   = "your_developer_token"
    client_id         = "your_client_id"
    client_secret     = "your_client_secret"
    refresh_token     = "your_refresh_token"
    customer_id       = "1234567890"
    login_customer_id = "9876543210"

    tables_configs = [
      {
        table_path = "google_ads.campaign"
        fields     = ["campaign.id", "campaign.name", "metrics.cost_micros"]
        filter     = "segments.date DURING LAST_7_DAYS"
      },
      {
        table_path  = "google_ads.ad_group"
        query       = "SELECT ad_group.id, ad_group.name FROM ad_group"
        customer_id = "2345678901"
      }
    ]
  }
}
```

## Limitations

- Batch only; no incremental/CDC reads (use `filter` with date segments for
  windowed extraction).
- No parallel reading; each job reads with a single split.
- No exactly-once semantics; re-running a job re-reads the data.
- Nested MESSAGE fields are emitted as JSON strings, not nested rows.

## Changelog

### next version

- Add Google Ads source connector with GAQL, automatic schema derivation and multi-table support
