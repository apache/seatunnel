import ChangeLog from '../changelog/connector-http-awin-advertiser.md';

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

# Awin Advertiser

Install `connector-http-awin-advertiser` and use the `AwinAdvertiser` source in a bounded job with parallelism 1. Use a personal or classic API token with access to the advertiser account. The source uses the HTTPS legacy advertiser API with a Bearer header. [Awin API authentication](https://help.awin.com/apis/apidocs/api-authentication)

## Resources

Paths beginning with `/reports`, `/transactions` and `/publishers` use the prefix `/advertisers/{advertiserId}`.

| Resource | GET path | Result |
| --- | --- | --- |
| `campaign_performance` | `/reports/campaign` | Daily campaign and publisher totals. Requests contain up to 400 dates. |
| `transactions` | `/transactions/` | Transactions and any available basket products. Requests contain up to 31 UTC dates. |
| `publisher_performance` | `/reports/publisher` | A separate publisher report for each UTC day. |
| `creative_performance` | `/reports/creative` | A separate report for each UTC day and selected Awin region. |
| `publishers` | `/publishers` | Publishers with an active relationship to the advertiser. |
| `accounts` | `/accounts?type=advertiser` | Advertiser accounts visible to the token's user. |

## Output

All six columns use STRING. The source emits INSERT rows.

| Column | Value |
| --- | --- |
| `resource` | Selected resource. |
| `record_key` | Provider ID for transactions, publishers and accounts. Reports use a JSON array of their identifying dimensions. |
| `advertiser_id` | Configured program ID, or the account ID from the accounts response. |
| `region` | Publisher report region when returned, or the requested creative region. |
| `report_date` | UTC report or transaction date. Null for undated resources. |
| `payload` | Full provider JSON, including nested data and exact numbers. Publisher and creative reports gain a date. Creative reports also gain a region. Transactions gain an `is_deleted` boolean that is true for a reported deleted commission status. |

For report keys, the dimension order is date, publisher, campaign for campaigns. It is date, publisher, region, currency for publisher performance and date, region, creative, publisher, currency for creative performance.

## Source options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `token` | `String` | — | Required. An Awin personal access token or classic API token. |
| `resources` | `List<String>` | — | Required. Unique resource names read in the given order. |
| `advertiser_id` | `Long` | — | Positive program ID. Required if any selected resource is scoped to an advertiser. |
| `start_date` | `String` | — | Inclusive first UTC date in YYYY-MM-DD. Required for reports and transactions. |
| `end_date` | `String` | Current UTC date | Inclusive final UTC date in YYYY-MM-DD, through today. A transaction request for today ends at the captured current UTC second. |
| `regions` | `List<String>` | `[]` | Required for creative performance. Select distinct codes from AT, AU, BE, BR, BU, CA, CH, DE, DK, ES, FI, FR, GB, IE, IT, NL, NO, PL, SE and US. |
| `resume_from` | `Map<String, String>` | `{}` | Previous UTC report date by selected resource key. For creative, use a key such as `creative_performance:GB`. |
| `lookback_days` | `Integer` | `0` | Days to rewind each supplied resume date, limited by start_date. Range 0 to 4000. |
| `campaign_window_days` | `Integer` | `400` | Maximum inclusive days per campaign request. Range 1 to 400. |
| `transaction_window_days` | `Integer` | `31` | Maximum inclusive UTC days per transaction request. Range 1 to 31. |
| `requests_per_minute` | `Integer` | `20` | Maximum HTTP attempts per reader in a rolling minute. Range 1 to 20. |
| `max_requests` | `Integer` | `10000` | Maximum report or metadata requests per scan, excluding retries. Range 1 to 100,000. |
| `max_attempts` | `Integer` | `4` | Total allowed attempts on a retryable request. Range 1 to 10. |
| `request_timeout_ms` | `Integer` | `90000` | Time budget for each acquisition including pacing, retries and decoding. Range 100 to 300,000. |
| `max_response_bytes` | `Integer` | `8388608` | Maximum decoded bytes in one response. Range 1,024 to 67,108,864. |
| `endpoint` | `String` | `https://api.awin.com` | Optional HTTPS origin with a trust certificate. Use an operator-trusted origin for the token. |
| `trust_certificate` | `String` | — | Worker-local PEM trust anchor for a custom origin. Hostname verification remains enabled. |

## Window, limits and recovery

- The scan freezes its UTC endpoint at split creation. All date ranges are inclusive. Each full transaction day ends at 23:59:59 UTC and the next request begins at the following midnight. Publisher and creative reports use single days so every aggregate can be assigned to its actual date.
- Supply `resume_from` for a new job when you have previously persisted synchronization dates. Each selected dated resource and creative region applies its own lookback once. A region without a resume date reads from `start_date`. Checkpoint recovery continues from the saved window and row position.
- Checkpoints verify a partially emitted report against a response digest. If the provider changes that response or the scope changes, restoration fails. The provider can change historical results between full requests. Downstream upserts or deduplication by resource, advertiser and record key let a later overlapping scan reconcile updates. The transaction flag reports the status the API returned for its requested date range.
- A documented legacy allowance is per Awin user and shared by other API clients using that user. This reader counts its own attempts and respects the server's retry delay. A partial HTTP response or unexpected continuation fails the scan. The legacy routes provide no pagination or isolated snapshot. We cannot detect a server-side omission with no signal. [Legacy limits and pagination](https://help.awin.com/advertisers/docs/en/comparison-between-shareasale-and-awin-apis)

## Example

The example runs a new scan of transactions and two creative regions. The GB report rereads two days before its supplied resume date. The US report starts from the configured first date.

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}
source {
  AwinAdvertiser {
    token = ${AWIN_TOKEN}
    advertiser_id = 123456
    resources = ["transactions", "creative_performance"]
    start_date = "2024-01-01"
    end_date = "2024-01-07"
    regions = ["GB", "US"]
    resume_from = { "creative_performance:GB" = "2024-01-05" }
    lookback_days = 2
    plugin_output = "awin_rows"
  }
}
sink {
  Console {
    plugin_input = "awin_rows"
  }
}
```

<ChangeLog />
