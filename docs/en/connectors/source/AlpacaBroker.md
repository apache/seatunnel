import ChangeLog from '../changelog/connector-http-alpaca-broker.md';

# AlpacaBroker

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

AlpacaBroker reads US Broker API resources using read-only HTTPS GET calls. Each source reads a bounded window with parallelism 1.

| Resource | Record ID | Output |
| --- | --- | --- |
| assets | Asset ID | Asset objects. Optional `asset_class` selects `us_equity`, `crypto`, `us_index`, or `crypto_perp`. |
| accounts_activities | Activity ID | Activities in ascending pages. |
| accounts | Account UUID | Accounts in an inclusive creation window. |
| account_documents | Document ID | Document metadata for every discovered account. |
| calendar | Trading date | Trading or settlement date window. |
| clock | Market timestamp | One market clock row. |
| country-info | Country code | One row per three-letter dictionary key. |

Every row has four string columns. `resource` and `record_id` identify the record. Document rows include `account_id`. `payload` keeps the JSON object, including nulls and precise decimals. Financial values represented as strings by the API remain strings.

## Authentication and transport

Set `username` and `password` to the Broker API Key ID and Secret. The sandbox uses `https://broker-api.sandbox.alpaca.markets`, and `LIVE` uses `https://broker-api.alpaca.markets`. A custom sandbox origin requires HTTPS and one worker-local PEM certificate. The source verifies the hostname and rejects redirects.

Retries share a request deadline and honor valid `Retry-After` or HTTP 429 `X-RateLimit-Reset` values. A delay beyond the deadline fails the request. The response bound applies after decompression. Error diagnostics avoid including credentials or provider payloads.

## Windows and recovery

Activity time bounds use whole UTC seconds. They apply to creation time and exclude both boundaries. The returned activity date can differ from creation time. The last ID on a nonempty page is the next token. An empty page ends the scan.

Account timestamps are inclusive with subsecond precision. The API returns at most 1,000 accounts. On a full page, the source holds back its final creation-time group and requests the same timestamp inclusively on the next page. If all 1,000 records share one timestamp, the scan fails because the API cannot establish the completeness of that group.

Accounts, document parent discovery, and activities retain the upper bound captured at split creation. Calendar and document dates are inclusive. When `end_date` is omitted, the source uses the UTC date at split creation. Document dates filter the children only. Document content download is outside the source's scope.

Checkpoints track the fixed upper bound, next cursor, pending parents, and terminal state. Restore requires the same resource, origin, key ID, filters, and activity page size. Concurrent updates can change API payloads during a scan. End-to-end delivery depends on the sink.

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `resource` | string | — | Broker API resource to read. |
| `environment` | string | `SANDBOX` | SANDBOX or LIVE US Broker API environment. |
| `username` | string | — | Broker API key ID used as the HTTP Basic username. |
| `password` | string | — | Broker API secret used as the HTTP Basic password. |
| `base_url` | string | — | Optional HTTPS origin for the sandbox environment. |
| `trust_certificate` | string | — | Worker-local PEM trust anchor required for a custom sandbox origin. |
| `start_time` | string | — | Lower timestamp for accounts or account activities, in RFC 3339. |
| `end_time` | string | — | Upper timestamp for accounts or account activities, in RFC 3339. |
| `start_date` | string | — | First inclusive calendar or document date in yyyy-MM-dd format. |
| `end_date` | string | — | Last inclusive calendar or document date in yyyy-MM-dd format. |
| `calendar_date_type` | string | `trading` | Calendar query interprets bounds as trading or settlement dates. |
| `asset_class` | string | — | Optional Broker API asset class filter. |
| `page_size` | int | `100` | Maximum records requested per activities page. |
| `max_requests` | int | `10000` | Maximum logical HTTP requests for one split. |
| `request_timeout_ms` | int | `30000` | Total time budget for each logical request, including retries and response decoding. |
| `max_attempts` | int | `3` | Maximum attempts for a retryable read. |
| `max_response_bytes` | int | `8388608` | Maximum bytes in a decoded successful response. |

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  AlpacaBroker {
    resource = "accounts_activities"
    environment = "SANDBOX"
    username = ${ALPACA_BROKER_KEY}
    password = ${ALPACA_BROKER_SECRET}
    start_time = "2024-02-29T00:00:00Z"
    end_time = "2024-03-01T00:00:00Z"
  }
}
sink {
  Console {}
}
```

<ChangeLog />
