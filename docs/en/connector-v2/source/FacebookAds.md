# FacebookAds

> Facebook Ads source connector

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

Reads data from Facebook (Meta) ad account edges (`campaigns`, `adsets`, `ads`,
`insights`, ...) using the Meta Marketing API (Graph API,
`GET /{version}/act_{ad_account_id}/{edge}`). Supports single-resource and
multi-table (`tables_configs`) batch ingestion. Results are streamed page by
page via the `paging.cursors.after` cursor, so only one page is held in memory
at a time.

Authentication uses a long-lived access token with the `ads_read` permission,
sent as an `Authorization: Bearer` header (never as a query parameter, so it
cannot leak into logs).

The Graph API has no field metadata service, so every output column is
**STRING** — Facebook already returns most metrics as JSON strings, and nested
objects/arrays are emitted as JSON text. The schema follows the `fields` list
order, so column `i` always corresponds to selected field `i`.

## Supported DataSource Info

| Datasource   | Supported Versions                                          |
|--------------|-------------------------------------------------------------|
| Facebook Ads | Graph API v23.0 (default, configurable via `api_version`)   |

## Prerequisites

1. A Meta developer app with the **Marketing API** product added.
2. An **access token** with the `ads_read` permission — e.g. a long-lived user
   token (60 days) or a system user token (non-expiring) from Business Manager.
3. The **ad account ID** to query (digits only, e.g. `1234567890`; a leading
   `act_` prefix is accepted and stripped).

## Source Options

| Name               | Type    | Required | Default                    | Description                                                                                          |
|--------------------|---------|----------|----------------------------|------------------------------------------------------------------------------------------------------|
| access_token       | String  | Yes      | -                          | Meta Marketing API access token with the `ads_read` permission.                                       |
| ad_account_id      | String  | Yes      | -                          | Ad account ID to query, digits only, e.g. `1234567890`. A leading `act_` prefix is accepted.          |
| api_version        | String  | No       | v23.0                      | Facebook Graph API version.                                                                           |
| resource           | String  | No*      | -                          | Ad account edge for single-table mode, e.g. `campaigns`, `adsets`, `ads`, `insights`. Requires `fields`. Exclusive with `tables_configs`. |
| fields             | List    | No       | -                          | Ordered list of field names to select, e.g. `[id, name, status]`. The output schema follows this order. |
| filtering          | String  | No       | -                          | JSON array passed as the Graph API `filtering` parameter, e.g. `[{"field":"effective_status","operator":"IN","value":["ACTIVE"]}]`. |
| params             | Map     | No       | -                          | Extra query parameters appended to the request, e.g. `{date_preset = last_30d, level = campaign}` for the `insights` edge. Must not contain `fields`, `limit`, `after`, `filtering` or `access_token`. |
| request_timeout_ms | Integer | No       | 60000                      | HTTP request timeout in milliseconds for a single call.                                               |
| max_retries        | Integer | No       | 3                          | Maximum retries for transient HTTP failures (429/5xx/rate limits/network errors) of a single request. |
| retry_backoff_ms   | Long    | No       | 1000                       | Base backoff in milliseconds between retries; doubled per attempt.                                    |
| page_size          | Integer | No       | -                          | Page size for each request (the Graph API `limit` parameter). When unset the server default is used.  |

\* Exactly one of `resource` or `tables_configs` must be provided.

The Graph API has no `SELECT *`, so the field list is always explicit via
`fields`. Invalid field names are rejected before any data is read, with the
offending name in the error message. Facebook rate limits (HTTP 400/403 with
error code 4, 17, 32 or 613, or HTTP 429) are retried with exponential backoff;
other client errors are never retried and the API error message is surfaced
as-is.

### tables_configs entry options

| Name          | Type   | Required | Description                                                                        |
|---------------|--------|----------|------------------------------------------------------------------------------------|
| table_path    | String | Yes      | Format: `database.resource`, e.g. `facebook_ads.campaigns`. The resource part names the edge to read. |
| fields        | List   | Yes      | Ordered field names for this table.                                                 |
| filtering     | String | No       | Graph API `filtering` JSON array for this table.                                    |
| params        | Map    | No       | Extra query parameters for this table.                                              |
| ad_account_id | String | No       | Per-table ad account ID override; falls back to the global `ad_account_id`.         |

## Data Type Mapping

| Facebook Ads Data Type | SeaTunnel Data Type | Notes                                                              |
|------------------------|---------------------|--------------------------------------------------------------------|
| Any scalar value       | STRING              | The Graph API serializes most metrics as JSON strings already.     |
| Object / array         | STRING              | Nested objects and arrays are emitted as JSON text.                |

Fields absent from a result row (the API omits empty fields entirely) are
emitted as `null`.

## Example

### Single resource

```hocon
source {
  FacebookAds {
    access_token  = "your_access_token"
    ad_account_id = "1234567890"

    resource = "campaigns"
    fields   = ["id", "name", "status", "objective", "created_time"]
    filtering = "[{\"field\":\"effective_status\",\"operator\":\"IN\",\"value\":[\"ACTIVE\"]}]"
  }
}
```

### Insights with extra parameters

```hocon
source {
  FacebookAds {
    access_token  = "your_access_token"
    ad_account_id = "1234567890"

    resource = "insights"
    fields   = ["campaign_id", "campaign_name", "impressions", "clicks", "spend"]
    params = {
      date_preset = "last_30d"
      level       = "campaign"
    }
  }
}
```

### Multiple tables (with per-table ad account ID)

```hocon
source {
  FacebookAds {
    access_token  = "your_access_token"
    ad_account_id = "1234567890"

    tables_configs = [
      {
        table_path = "facebook_ads.campaigns"
        fields     = ["id", "name", "status"]
      },
      {
        table_path    = "facebook_ads.insights"
        fields        = ["campaign_id", "impressions", "spend"]
        params        = { date_preset = "last_30d", level = "campaign" }
        ad_account_id = "2345678901"
      }
    ]
  }
}
```

## Limitations

- Batch only; no incremental/CDC reads (use `params` with `time_range` or
  `date_preset` for windowed extraction on the `insights` edge).
- No parallel reading; each job reads with a single split.
- No exactly-once semantics; re-running a job re-reads the data.
- All columns are STRING; nested objects are emitted as JSON strings, not
  nested rows.

## Changelog

### next version

- Add Facebook Ads source connector with cursor pagination and multi-table support
