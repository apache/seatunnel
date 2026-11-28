<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

import ChangeLog from '../changelog/connector-http-app-store-connect.md';

# App Store Connect

Install `connector-http-app-store-connect` and use `AppStoreConnect` in a batch job. Provide an App Store Connect team API key with access to the selected apps and reports. Sales and finance have separate role requirements. Create the ongoing or one-time analytics report requests in App Store Connect before reading them. The source discovers and downloads existing report segments. [Team API token requirements](https://developer.apple.com/documentation/appstoreconnectapi/generating-tokens-for-api-requests) · [Analytics report lifecycle](https://developer.apple.com/documentation/appstoreconnectapi/downloading-analytics-reports)

## Resources

| Selection | Data |
| --- | --- |
| `list_id_apps`, `customer_reviews_per_app` | Apps and per-app reviews |
| `sales_report`, `subscriber_report`, `subscription_report`, `subscription_event_report` | Sales and Trends daily reports |
| `finance_report` | Monthly consolidated `FINANCIAL` report for region `ZZ` |
| `analytics_report_requests_ongoing`, `analytics_report_requests_historical` | Per-app `ONGOING` and `ONE_TIME_SNAPSHOT` requests |
| `analytics_installations_reports`, `analytics_installations_instances`, `analytics_installations_segments`, `analytics_installations_segment_details`, `app_store_installations_and_deletions` | Installation and deletion standard metadata, followed by downloaded TSV rows |
| `analytics_app_download_reports`, `analytics_app_download_instances`, `analytics_app_download_segments`, `analytics_app_download_segment_details`, `app_download` | App Downloads standard metadata, followed by downloaded TSV rows |

Append `_historical` to any of the last ten names to select the corresponding snapshot dataset. Selecting a child automatically reads its parent collections. Segment list and detail rows omit the temporary download URL. The daily sales summary uses version `1_0`. Subscriber, subscription and subscription event reports use `1_3`. [Sales report versions](https://developer.apple.com/documentation/appstoreconnectapi/get-v1-salesreports) · [Financial reports](https://developer.apple.com/help/app-store-connect/getting-paid/download-financial-reports)

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `issuer_id` | string | required | Team key issuer ID |
| `key_id` | string | required | Team key ID |
| `secret_key` | string | required | PKCS#8 P-256 private key as PEM or base64 DER |
| `vendor_number` | string | none | Required for the five Sales and Trends or financial datasets |
| `resources` | list of string | all | Resource names in the requested read order |
| `sales_reports_start_date` | string | 365 days before the batch UTC date | Inclusive `YYYY-MM-DD` start for sales, subscriber and subscription reports. Reads through yesterday. |
| `subscription_event_reports_start_date` | string | 365 days before the batch UTC date | Inclusive `YYYY-MM-DD` start for subscription events. Reads through two days before the batch. |
| `subscription_event_skip_dates` | list of string | empty | `YYYY-MM-DD` dates omitted from subscription event requests |
| `finance_reports_start_date` | string | 11 months before the batch UTC month | Inclusive `YYYY-MM` fiscal report month. Reads through the month containing the UTC date 35 days before the batch. |
| `analytics_reports_start_date` | string | 35 days before the batch UTC date | Inclusive processing date for ongoing analytics. Reads through yesterday. Historical requests read all available daily instances. |
| `reviews_start_date` | string | none | Optional inclusive ISO 8601 timestamp with offset. Reads through the batch start time. |
| `api_origin` | string | `https://api.appstoreconnect.apple.com` | HTTPS API origin. The team JWT is sent there. |
| `download_hosts` | list of string | empty | Additional exact HTTPS hosts for signed download URLs. Standard Amazon S3 HTTPS hosts are accepted by default. |
| `page_size` | integer | 200 | Maximum records requested per collection page, from 1 through 200 |
| `max_pages_per_parent` | integer | 1000 | Page limit per relationship and parent |
| `min_request_interval_ms` | integer | 100 | Minimum interval between HTTPS attempts |
| `request_timeout_ms` | integer | 30000 | Timeout for each HTTPS attempt, including body processing |
| `operation_timeout_ms` | integer | 120000 | Operation timeout across attempts, signing renewal and download URL refresh |
| `max_retries` | integer | 3 | Additional attempts for transient statuses or GET connection failures |
| `max_json_bytes` | integer | 4194304 | Limit for metadata JSON transfer and decoded body bytes |
| `max_compressed_report_bytes` | integer | 33554432 | Limit for report transfer bytes |
| `max_decoded_report_bytes` | integer | 134217728 | Limit for decompressed report bytes |
| `max_report_row_chars` | integer | 262144 | Limit for UTF-16 code units in one report record |

Sales start dates older than 365 days and finance start months older than 11 months are clamped to those bounds. Apple controls report availability and retention. Start dates select candidate periods. A fresh batch reads the configured date range again.

## Output and recovery

The schema contains string columns `resource`, `id`, `app_id`, `request_id`, `report_id`, `instance_id`, `segment_id`, `report_date`, and `record`. Lineage and date are nullable. For analytics, `report_date` is the instance processing date, while the event date is the TSV column named `Date`. `record` is JSON containing Apple metadata or the complete report row keyed by TSV header. TSV cells are strings, including empty fields, identifiers with leading zeros, and exact decimal amounts. IDs identify API objects or rows within a particular dated report or segment.

Each batch uses one active source reader. Checkpoints save the selected resource, frozen date window, active API page positions and report row offset. The source verifies saved page and report fingerprints before resuming an offset. If Apple has changed or removed the saved data, the source fails with a recovery diagnostic. An available download URL is obtained again from its segment ID at restore. Sink delivery depends on the selected sink's checkpoint support.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  AppStoreConnect {
    issuer_id = "<team-issuer-id>"
    key_id = "<team-key-id>"
    secret_key = ${APP_STORE_CONNECT_SECRET_KEY}
    resources = ["list_id_apps", "customer_reviews_per_app"]
    reviews_start_date = "2026-01-01T00:00:00Z"
  }
}

sink {
  Console {}
}
```

Set `secret_key` through your job configuration's secret injection mechanism. The engine must trust the HTTPS certificates for the API and its download hosts.

<ChangeLog />
