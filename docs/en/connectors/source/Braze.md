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

import ChangeLog from '../changelog/connector-http-braze.md';

# Braze

Install `connector-http-braze`. The Braze source reads bounded JSON exports from one Braze workspace. Configure one source block per resource with source parallelism set to `1`. The workspace REST API key needs the permission for the selected endpoint and for the parent listing used by nested exports.

## Resources

| `resource` | Braze GET path | Output |
| --- | --- | --- |
| `campaigns` | `/campaigns/list` | One campaign object per row |
| `campaigns_analytics` | `/campaigns/data_series` | Daily campaign metrics, including message variants |
| `campaigns_details` | `/campaigns/details` | One details object per discovered campaign |
| `canvases` | `/canvas/list` | One Canvas object per row |
| `canvases_analytics` | `/canvas/data_series` | Daily totals and available step and variant breakdowns |
| `canvases_details` | `/canvas/details` | One details object per discovered Canvas |
| `events` | `/events/list` | One `{"event_name":"…"}` object per event name |
| `events_analytics` | `/events/data_series` | Daily occurrence counts for each discovered event |
| `kpi_daily_new_users` | `/kpi/new_users/data_series` | Daily workspace new-user counts |
| `kpi_daily_active_users` | `/kpi/dau/data_series` | Daily workspace active-user counts |
| `kpi_daily_app_uninstalls` | `/kpi/uninstalls/data_series` | Daily workspace uninstall counts |
| `cards` | `/feed/list` | Legacy News Feed card objects |
| `cards_analytics` | `/feed/data_series` | Legacy daily News Feed engagement metrics |
| `segments` | `/segments/list` | One segment object per row |
| `segments_analytics` | `/segments/data_series` | Daily segment size estimates |
| `segments_details` | `/segments/details` | One details object per discovered segment |

The two legacy News Feed resources require continued access to the historical News Feed endpoints in your workspace. Current Braze export documentation omits these endpoints. A response with HTTP 404 or 410 fails the job with an explicit legacy-availability error. Content Cards are a separate resource. [Current Braze export reference](https://www.braze.com/docs/api/endpoints/export) · [Historical News Feed reference](https://www.postman.com/braze-inc/braze-public-workspace/request/mqcth3a/news-feed-card-analytics)

## Configuration

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `instance` | string | Required | Dashboard instance, such as `us-01` or `eu-01`. Supported values are `us-01` through `us-08`, `us-10`, `eu-01`, `eu-02`, `au-01`, `id-01`, `jp-01` and `kr-01`. Matching is case-insensitive. |
| `token` | string | Required | Workspace REST API key, sent as an HTTPS Bearer credential to the chosen instance. |
| `resource` | string | Required | One export name from the resource table. |
| `start_date` | string | None | `YYYY-MM-DD`. Required as the inclusive first day for analytics and KPI resources. For direct `campaigns`, `canvases` and `cards` lists, an optional exclusive last-edit threshold at UTC midnight. |
| `end_date` | string | Current UTC day | `YYYY-MM-DD`. Inclusive final UTC day for analytics and KPIs only. An end on today's date includes the partial day up to scan start. |
| `max_pages` | int | `2000` | Read up to this many numbered pages, including parent discovery and any final empty-page probe. Range 1–20000. |
| `connect_timeout_ms` | int | `10000` | TCP connection timeout in milliseconds. Range 1–120000. |
| `read_timeout_ms` | int | `30000` | Socket read timeout in milliseconds. Range 1–120000. |
| `request_deadline_ms` | int | `60000` | Total per-request budget for connection, TLS, reading and retry waits. Range 1–300000. |
| `max_attempts` | int | `4` | Maximum attempts, including the first, for retryable HTTP statuses. Range 1–10. |
| `max_response_bytes` | int | `8388608` | Maximum encoded and decoded response size in bytes. Range 1024–33554432. |

Analytics intervals must start on or after 2000-01-01, with both bounds on or before the current UTC day. Analytics use contiguous, inclusive windows of up to 100 days, or 14 days for Canvas analytics. Parent discovery uses the full provider list without an edit-date filter. The provider's default archive visibility still applies. Campaign, Canvas and Segment lists use groups of 100, custom event names use groups of 250, and the historical News Feed list continues until the first empty page. [Braze REST instances and permissions](https://www.braze.com/docs/api/basics)

## Output and recovery

Each output row has three string fields.

| Field | Value |
| --- | --- |
| `entity_id` | The list object's ID, custom event name, or the parent ID for details and analytics. Null for workspace KPIs. |
| `time` | The provider's ISO-8601 time in an analytics or KPI record. Null for lists and details. |
| `data` | The complete JSON record, including nulls, nested fields and numeric values. Child records include their `campaign_id`, `canvas_id`, `event_name`, `card_id` or `segment_id`. |

A checkpoint keeps the initial analytics cutoff, the numbered page and parent and record offsets. On restore, the source refetches the required page and checks the order of the active parent IDs and any partly emitted records before continuing. A changed response at either saved position fails explicitly. Braze lists and metrics can change during a scan, and the API offers no immutable snapshot for these endpoints. Configure a new date range for a new extraction run.

The source verifies HTTPS certificates and hostnames, disables redirects, and requires a Braze success envelope for HTTP 200. HTTP 408, 429 and 5xx retries use the remaining deadline. Throttling honors the Braze retry-delay or reset headers. [Braze rate limits](https://www.braze.com/docs/api/api_limits)

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Braze {
    instance = "us-01"
    token = ${BRAZE_API_KEY}
    resource = "canvases_analytics"
    start_date = "2024-01-01"
    end_date = "2024-01-16"
  }
}
sink {
  Console {}
}
```

<ChangeLog />
