import ChangeLog from '../changelog/connector-http-appfigures.md';

# Appfigures

{/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */}

Appfigures reads one Appfigures v2 resource in a bounded batch job. Use parallelism 1. Create a personal access token for your account and grant its API client the required scopes. [Personal Access Tokens](https://docs.appfigures.com/guides/authentication/personal-access-tokens/)

## Resources

| Resource | API route | Rows |
| --- | --- | --- |
| `status` | `/` | Authenticated user from the `user` field. |
| `data_categories` | `/data/categories` | One entry per category ID. |
| `users` | `/users` | One user per result, with pagination and optional login-time filtering. |
| `usage` | `/usage` | One API request and partner-credit usage record. |
| `products_mine_search` | `/products/mine` | One account product per dictionary entry or paginated result. |
| `reports_sales` | `/reports/sales` | Sales totals or grouped report rows. |
| `reports_revenue` | `/reports/revenue` | Revenue totals or grouped report rows. |
| `reports_subscriptions` | `/reports/subscriptions` | Subscription totals or grouped report rows. |
| `reports_ads` | `/reports/ads` | Ad publisher totals or grouped report rows. |
| `reports_adspend` | `/reports/adspend` | Advertiser spend totals or grouped report rows. |
| `reports_ratings` | `/reports/ratings` | Rating totals or grouped report rows. |

Read account products with `products:read`. The user list requires account read access and an administrator for other account users. Sales, revenue, subscriptions, ads and ad spend use `private:read`. Ratings use `public:read` and may depend on the account's Public Data API plan. [Users](https://docs.appfigures.com/reference/users/) · [Products](https://docs.appfigures.com/reference/products/) · [Ratings](https://docs.appfigures.com/reference/reports-ratings/)

## Output

| Column | Type | Meaning |
| --- | --- | --- |
| `resource` | String | Selected canonical resource name. |
| `record_id` | String | User, product or category ID, `usage` for API usage, or `total` for an ungrouped report. Grouped reports use a digest of the full returned key path. |
| `dimension_keys` | String | JSON array of keys in the provider's nesting order. Dictionary entries contain their entry key. Other rows without enclosing keys use `[]`. |
| `payload` | String | JSON object with provider fields, nested data and nulls. Numbers retain their precision, and money supplied as decimal text stays text. |

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `resource` | String | Yes | — | Selects one of the resources above. |
| `token` | String | Yes | — | Appfigures personal access token. |
| `search_store` | String | No | `apple` for account products | Appfigures store identifier, such as `apple` or `google_play`. Reports use the API's default product selection when this option is absent. |
| `group_by` | String | No | empty | Comma-separated report grouping pivots. Empty uses the provider's combined total. |
| `report_start_date` | String | No | — | Inclusive report start date, in `YYYY-MM-DD` form. Provide both dates to select an explicit window. |
| `report_end_date` | String | No | — | Inclusive report end date, in `YYYY-MM-DD` form. |
| `user_start_time` | String | No | — | Inclusive local `last_login` lower bound for users. |
| `user_end_time` | String | No | — | Exclusive local `last_login` upper bound for users. |
| `page_size` | Integer | No | `10` | Users page size from 1 to 100. If account products return a paginated envelope, the continuation keeps the initially advertised page size. |
| `max_pages` | Integer | No | `1000` | Maximum pages in a paginated resource, from 1 to 10000. |
| `max_rows` | Integer | No | `100000` | Maximum records in a response or paginated resource, from 1 to 500000. |
| `max_response_bytes` | Integer | No | `8388608` | Maximum decoded bytes in a response, from 1024 to 67108864. |
| `max_attempts` | Integer | No | `4` | Maximum attempts of a retryable request, from 1 to 10. |
| `request_timeout_ms` | Integer | No | `30000` | Request budget including retries, from 100 to 300000 milliseconds. |
| `endpoint` | String | No | `https://api.appfigures.com` | HTTPS origin. A custom value requires `trust_certificate`. The connector adds the v2 API path. |
| `trust_certificate` | String | No | — | Worker-local PEM trust anchor for a custom origin. Hostname verification still applies. |

## Reports and login ranges

- The `product`, `country`, and `date` pivots are available on all six reports. Use `store` on Sales, Revenue, Subscriptions, Ads and Ad Spend. Use `network` on Ads and Ad Spend and `device` on Sales. Synonymous plural pivot names are accepted. Report rows retain the original grouping path and the provider's leaf fields. [Sales](https://docs.appfigures.com/reference/sales/) · [Revenue](https://docs.appfigures.com/reference/revenue/) · [Subscriptions](https://docs.appfigures.com/reference/subscriptions/) · [Ads](https://docs.appfigures.com/reference/ads/) · [Ad Spend](https://docs.appfigures.com/reference/ad-spend/)
- With a `date` pivot and no specified dates, the source fixes the last 30 completed UTC calendar days at job start. Other requests with omitted dates use Appfigures defaults. Explicit Sales ranges can contain up to 30 inclusive days. Appfigures can return a different lifetime total than the sum of daily results.
- A report `search_store` uses the Appfigures account products route to resolve the selected store's active product IDs before querying the corresponding report. This also requires `products:read`. An empty selection yields no rows. The source fails if the product selection exceeds the URL budget, so each report's totals and ratios come from a single provider calculation.
- User login filters compare local timestamps such as `2025-11-01T12:30:00` as returned by Appfigures. Apply the filter only with bounds intended for those wall-clock values. The original timestamp and timezone fields stay in the payload. Users with no last login appear in unfiltered reads.

## Recovery and failures

Zeta checkpoints store the current page or report selection and the offset and digest of a partly emitted response. On restore, a changed partial response, changed read configuration, or inconsistent pagination fails the job. Appfigures can change account records during a scan. Older checkpoints can replay output.

A denied scope, depleted Public Data credits, malformed payload or exhausted daily quota fails explicitly. HTTP 420 is the daily API limit. Temporary HTTP 429 and server errors use bounded retries within the request deadline. HTTPS requests stay on the configured origin. Diagnostics omit credentials and response bodies. [Rate Limits](https://docs.appfigures.com/rate-limits/)

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Appfigures {
    resource = "reports_sales"
    token = ${APPFIGURES_TOKEN}
    search_store = "apple"
    group_by = "product,country"
    report_start_date = "2026-01-01"
    report_end_date = "2026-01-30"
  }
}
sink {
  Console {}
}
```

<ChangeLog />
