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

import ChangeLog from '../changelog/connector-http-aircall.md';

# Aircall

Install `connector-http-aircall` and configure a batch job with `Aircall`. Supply the API ID and API token from your Aircall Dashboard. The connector uses Basic authentication for a private account integration, verifies HTTPS certificates and hostnames, and rejects redirects. [Aircall authentication](https://developer.aircall.io/docs/authentication)

## Resources

| Resource | GET route under `/v1` | Selection |
| --- | --- | --- |
| `calls` | `/calls` | Standard |
| `company` | `/company` | Standard |
| `contacts` | `/contacts` | Standard |
| `numbers` | `/numbers` | Standard |
| `tags` | `/tags` | Standard |
| `user_availability` | `/users/availabilities` | Standard |
| `users` | `/users` | Standard |
| `teams` | `/teams` | Standard |
| `webhooks` | `/webhooks` | Standard |
| `conversations` | `/conversations` | Standard |
| `conversation_engagements` | `/conversations/{id}/engagements` | Standard |
| `campaigns` | `/campaigns` | Standard |
| `campaign_outcomes` | `/campaign_outcomes` | Standard |
| `trackers` | `/trackers` | Standard |
| `sms_templates` | `/sms/templates` | Standard |
| `whatsapp_templates` | `/numbers/{id}/templates` | Standard |
| `user_numbers` | `/users/{id}/numbers` | Standard |
| `dialer_campaigns` | `/users/{id}/dialer_campaign` | Standard |
| `dialer_campaign_phone_numbers` | `/users/{id}/dialer_campaign/phone_numbers` | Standard |
| `call_transcriptions` | `/calls/{id}/transcription` | AI opt-in |
| `call_sentiments` | `/calls/{id}/sentiments` | AI opt-in |
| `call_topics` | `/calls/{id}/topics` | AI opt-in |
| `call_summaries` | `/calls/{id}/summary` | AI opt-in |
| `call_action_items` | `/calls/{id}/action_items` | AI opt-in |
| `call_playbook_results` | `/calls/{id}/playbook_result` | AI opt-in |

With no explicit resource list, the scan includes all 19 standard resources. Set `include_conversation_intelligence = true` to also enable the six AI resources. Selected endpoints require the corresponding Aircall permissions and plan. Playbook results require AI Assist Pro. Choose `resources` to limit a job to endpoints enrolled in your account. The scan discovers the necessary calls, numbers, users or conversations when their child resources are selected. A provider response identifying an unavailable asset, inactive dialer campaign or unconfigured WhatsApp line can end that particular child request. Authentication and plan permission errors fail the scan. [Aircall API reference](https://developers.aircall.io/api-references) · [Playbook results](https://developer.aircall.io/docs/ai-playbook-results)

Rows contain `resource STRING`, `id STRING`, nullable `parent_id STRING`, nullable `call_started_at BIGINT` in epoch seconds, and `record STRING`. The last column preserves the complete provider record as JSON with nested values, Unicode, null and numeric precision. Webhooks use `webhook_id` as their ID. The source outputs metadata URLs contained in the record as values.

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `username` | String | Required | API ID |
| `password` | String | Required | API token |
| `base_url` | String | `https://api.aircall.io` | HTTPS origin before `/v1` |
| `resources` | List of strings | All enabled | Unique resource names for the job |
| `include_conversation_intelligence` | Boolean | `false` | Enable the optional AI resources |
| `start_time` | String | Resource-specific | UTC history lower bound at whole-second precision |
| `end_time` | String | Coordinator start time | Exclusive cutoff for the dated scan, preserved on restore |
| `lookback_days` | Integer | `3` | Re-read overlap for calls and conversations, 0–30 days |
| `call_window_days` | Integer | `1` | Initial call query span, 1–365 days |
| `contact_window_days` | Integer | `30` | Initial contact query span, 1–365 days |
| `conversation_window_days` | Integer | `30` | Initial conversation query span, 1–365 days |
| `page_size` | Integer | `50` | Standard and campaign page size, 1–50 |
| `sms_page_size` | Integer | `100` | SMS template page size, 1–100 |
| `max_pages` | Integer | `10000` | Maximum pages per collection partition, 1–10000 |
| `request_interval_ms` | Integer | `550` | Minimum interval between this job's requests, 500–60000 ms |
| `max_attempts` | Integer | `4` | Attempts per page including the first, 1–8 |
| `socket_timeout_ms` | Integer | `15000` | Connect and read inactivity timeout, 100–120000 ms |
| `page_deadline_ms` | Integer | `60000` | Elapsed budget for fetch, retry and parsing, 500–600000 ms |
| `max_response_bytes` | Integer | `8388608` | Maximum decompressed bytes, 128–33554432 |

## Time ranges, pagination and recovery

Calls default to a 365-day history plus overlap. The provider's list API exposes only its retained six months. Call query ranges use creation time. The calls export checks `started_at` against the overall history interval, so a record can be acquired in one creation partition and have a different start time. Increase the overlap or history range if relevant calls can start later than their creation range. Contacts and conversations default to January 2014. Their initial scan is partitioned into 30-day requests. The conversations date filter uses `last_engagement_at`, which does not change on assignment or status updates. Engagement lists are read from the discovered conversations without child date filters.

The connector validates numbered continuation links, and uses scoped campaign cursors and WhatsApp tokens. It subdivides a date-filterable query before its result ceiling could truncate data. If a single second or a route without a supported time filter can exceed 10,000 entries, the job fails. Retries follow valid vendor reset and retry headers within the deadline. The company quota is shared with other users of the API. [Aircall pagination limit](https://support.aircall.io/en-gb/articles/34702023578397) · [Aircall API reference](https://developers.aircall.io/api-references)

A checkpoint preserves the cutoff, current parent and child positions, and digests for partially emitted pages. Restore with the same origin, resource selection, query windows, and page sizes. The source validates saved pages before skipping previous rows. Collection changes can affect later results, and the lookback may revisit records from a previous job. The API does not provide a cross-page snapshot.

## Example

```hocon
env {
  job.mode = "BATCH"
}

source {
  Aircall {
    username = "${aircall_api_id}"
    password = "${aircall_api_token}"
    resources = ["calls", "contacts", "call_transcriptions"]
    include_conversation_intelligence = true
    start_time = "2026-01-01T00:00:00Z"
    end_time = "2026-02-01T00:00:00Z"
  }
}

sink {
  Console {}
}
```

<ChangeLog />
