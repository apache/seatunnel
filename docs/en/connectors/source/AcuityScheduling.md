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

import ChangeLog from '../changelog/connector-http-acuity-scheduling.md';

# Acuity Scheduling

The `AcuityScheduling` source reads seven Acuity Scheduling API v1 resources as a bounded job. Set the source parallelism to one. The end-to-end suite targets SeaTunnel Zeta.

Authenticate with the numeric User ID and API key from Acuity Integrations. The source sends HTTP Basic credentials over HTTPS, verifies the host, and uses the worker JVM trust store. [Acuity authentication](https://developers.acuityscheduling.com/reference/quick-start)

## Source options

| Name | Type | Default | Description |
| --- | --- | --- | --- |
| `api_url` | String | `https://acuityscheduling.com` | Trusted HTTPS origin. The connector adds the fixed API path. |
| `username` | String |  | Required numeric Acuity User ID. |
| `password` | String |  | Required Acuity API key. Supply through the deployment's secret mechanism. |
| `resources` | List&lt;String&gt; | All seven | Ordered list of resource names to read. Names must be distinct. |
| `start_date` | String |  | Inclusive lower date. Required for appointments and blocks. |
| `end_date` | String | Current date | Inclusive upper date. The default is evaluated once in timezone at start and retained across restore. |
| `resume_date` | String |  | Optional prior successful date boundary. Supply it to reread the preceding lookback interval. |
| `timezone` | String | `UTC` | IANA business or calendar timezone used to choose a default end_date. This option does not change the provider timezone. |
| `lookback_days` | Integer | `7` | Days to rewind resume_date, clipped at start_date. Range 0–90. |
| `window_days` | Integer | `31` | Maximum days in each initial window. Range 1–365. |
| `max_date_results` | Integer | `100` | Requested max per appointments or blocks query. Range 1–100. |
| `calendar_id` | Integer |  | Optional positive calendar ID for appointments and blocks only. |
| `max_requests` | Integer | `10000` | Maximum logical requests in the crawl, including date-window probes. Range 1–50,000. |
| `max_response_bytes` | Integer | `4194304` | Separate maximums for wire and decompressed response bytes. Range 1,024–16,777,216. |
| `max_records_per_response` | Integer | `20000` | Row limit for each non-date list. Range 1–50,000. |
| `connect_timeout_ms` | Integer | `10000` | TCP connect timeout in milliseconds. Range 1–300,000. |
| `read_timeout_ms` | Integer | `10000` | Inactivity timeout for TLS handshake and response reads. Range 1–300,000 ms. |
| `request_timeout_ms` | Integer | `30000` | Total response deadline including retries and waits. Range 1–300,000 ms. |
| `max_attempts` | Integer | `3` | Maximum HTTP attempts per request including the first. Range 1–10. |

Use `YYYY-MM-DD` for date options. Set `timezone` to the business or selected calendar's zone when using the default end date.

## Resources

| Resource | Read behavior |
| --- | --- |
| [`appointments`](https://developers.acuityscheduling.com/reference/get-appointments) | Dated. Includes canceled appointments and embedded intake form answers. |
| [`calendars`](https://developers.acuityscheduling.com/reference/get-calendars) | Full list of accessible calendars. |
| [`clients`](https://developers.acuityscheduling.com/reference/clients) | Full client list. |
| [`appointment-types`](https://developers.acuityscheduling.com/reference/appointment-types) | Full appointment type list. |
| [`blocks`](https://developers.acuityscheduling.com/reference/blocks) | Dated calendar blocks. |
| [`labels`](https://developers.acuityscheduling.com/reference/labels) | Full label list. |
| [`forms`](https://developers.acuityscheduling.com/reference/forms) | Full intake form definitions and fields. |

Each output row has five STRING columns. The `resource` and `record_id` identify the object. The `data` column contains complete provider JSON, including nested forms, numeric values, and nulls. For a client without an ID, `record_id` hashes the provider email when available and otherwise hashes the full object. The nullable `window_start` and `window_end` show the inclusive query window for appointments and blocks. Payloads may contain contact details and form answers.

## Date windows and recovery

Appointments and blocks filter their scheduled calendar date using `minDate` and `maxDate`. The date bounds describe the service query and do not represent record creation or modification times. The connector uses civil dates, including during daylight-saving transitions. With a `resume_date`, it rereads from the later of `start_date` and `resume_date - lookback_days`. Records outside the reread date range can still change or be rescheduled. Deduplicate overlapping runs using the resource and record identity.

A window reaching the requested maximum is subdivided without emitting the probe response. A single day that still reaches the maximum fails with its date and endpoint so a saturated day cannot silently truncate data. Use `calendar_id` for a job restricted to one calendar. The other five endpoints use full-list requests and enforce response limits.

A checkpoint retains the resolved upper date, pending windows, and progress through the resources. The source resumes the next unfinished query with the same account, endpoint, resources, and date options. The API can change between queries and checkpoint recovery may replay rows since the last completed checkpoint.

The connector retries HTTP 429 and selected 5xx responses within the attempt and total request limits. It honors a valid `Retry-After`, checks compressed and decoded response sizes, and fails on redirects. Diagnostic messages withhold credentials and response details.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  AcuityScheduling {
    username = ${ACUITY_USER_ID}
    password = ${ACUITY_API_KEY}
    resources = ["appointments", "blocks", "forms"]
    start_date = "2025-01-01"
    end_date = "2025-03-31"
    timezone = "America/New_York"
  }
}

sink {
  Console {}
}
```

<ChangeLog />
