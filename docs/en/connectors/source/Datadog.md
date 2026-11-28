<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements. See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License. You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->


import ChangeLog from '../changelog/connector-http-datadog.md';

# Datadog

The Datadog source reads one resource per source stanza in batch mode. Define multiple source
stanzas in a job to read resources with different scopes or time ranges.

## Resources

| Resource | Endpoint | Application-key permission |
| --- | --- | --- |
| `audit_logs` | GET `/api/v2/audit/events` | `audit_logs_read` |
| `dashboards` | GET `/api/v1/dashboard` | `dashboards_read` |
| `downtimes` | GET `/api/v1/downtime` | `monitors_read` |
| `teams` | GET `/api/v2/team` | `teams_read` |
| `incidents` | GET `/api/v2/incidents` | `incident_read` |
| `logs` | GET `/api/v2/logs/events` | `logs_read_data` |
| `metrics` | GET `/api/v2/metrics` | `metrics_read` |
| `monitors` | GET `/api/v1/monitor` | `monitors_read` |
| `service_level_objectives` | GET `/api/v1/slo` | `slos_read` |
| `synthetic_tests` | GET `/api/v1/synthetics/tests` | `synthetics_read` |
| `users` | GET `/api/v2/users` | `user_access_read` |

The dashboards endpoint returns summaries. Metrics returns the metric catalog and its available
metadata. Downtimes uses the v1 list, which returns one array. Monitors uses the v1 ID-offset
walk. The response limit also applies to these two endpoint modes. The incidents endpoint is in
public beta. `teams` reads modern general organization teams. The retired legacy incident-teams
endpoint `/api/v2/teams` is not implemented, and `incident_teams` is not an accepted resource name.
The application key needs `teams_read` and visibility of the organization teams you intend to
read. Availability, visibility and retention depend on your Datadog site and organization.

## Options

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `resource` | String | Yes | — | One of the resources above. |
| `access_key` | String | Yes | — | Datadog API key. |
| `secret_key` | String | Yes | — | Datadog application key with permission for the resource. |
| `site` | String | No | `datadoghq.com` | Site parameter. Also accepts `us3.datadoghq.com`, `us5.datadoghq.com`, `datadoghq.eu`, `ap1.datadoghq.com`, `ap2.datadoghq.com`, `uk1.datadoghq.com`, `ddog-gov.com`, and `us2.ddog-gov.com`. |
| `start_time` | String | For logs and audit logs | — | Inclusive starting ISO-8601 instant. |
| `end_time` | String | No | Initial job time | Exclusive ending ISO-8601 instant. An empty value uses the default. Applies to logs and audit logs. |
| `logs_query` | String | No | Empty | Datadog log search expression, up to 4,096 characters. Applies only to logs. |
| `slice_days` | Integer | No | `1` | Event window size in days, from 1 to 31. A total configured interval can span up to 36,525 days. |
| `metrics_window_seconds` | Integer | No | `5184000` | Active-reporting metric catalog lookback from 0 to 5,184,000 seconds. Zero returns only metrics reported in the last few minutes; it does not request an unbounded history. |
| `page_size` | Integer | No | `100` | Requested rows per page, from 1 to 1,000. Smaller endpoint limits take precedence. Downtimes and the monitor ID walk do not take this parameter. |
| `request_timeout_ms` | Integer | No | `10000` | Connection and socket idle timeout, from 100 to 60,000 ms. |
| `page_deadline_ms` | Integer | No | `120000` | Maximum elapsed time for a page including retry waits, from 100 to 300,000 ms. |
| `max_attempts` | Integer | No | `4` | Total attempts for a read-only page, from 1 to 8. |
| `max_response_bytes` | Integer | No | `8388608` | Maximum decompressed response size, from 1,024 to 16,777,216 bytes. |

## Output and recovery

All four columns are strings. `resource` identifies the resource. `id` is the provider
identifier, the public ID of a synthetic test, or the name of a scalar metric record. `timestamp`
holds the original event timestamp for logs and audit logs. `id` and `timestamp` can be null.
`record` contains the individual provider value as JSON, including a JSON string for a scalar
metric name. JSON whitespace and number notation may be normalized. Nested values, numbers and
Unicode are preserved.

Event windows include their start and exclude their end. Equal bounds return no records. The
source preserves distinct events that share a timestamp. The first job run freezes its end time
and a compatible recovery resumes the saved page or window. Credentials and settings that
determine the query scope must match an existing checkpoint. Each new independent job reads the
configured range and has no external watermark. Use an overlapping range and an idempotent sink
if your scheduled jobs must account for late visibility.

This source has one active reader per stanza. Recovery may replay rows emitted after the
checkpoint. Datadog's pagination reflects live data, so changes, retention, and expired cursors
can affect reads. An expired cursor or changed checkpointed page prefix fails the job. Incomplete
or forbidden responses also fail. Bounds apply to each response and to the total pages per
resource or event window. HTTP 429 can delay a retry within the page deadline.

## Example

```hocon
env {
  job.mode = "BATCH"
  checkpoint.interval = 1000
}
source {
  Datadog {
    resource = "logs"
    site = "datadoghq.com"
    access_key = ${DATADOG_ACCESS_KEY}
    secret_key = ${DATADOG_SECRET_KEY}
    start_time = "2026-01-01T00:00:00Z"
    end_time = "2026-01-02T00:00:00Z"
    logs_query = "service:checkout"
  }
}
sink {
  Console {}
}
```

[Datadog sites](https://docs.datadoghq.com/getting_started/site/) ·
[Datadog API](https://docs.datadoghq.com/api/latest/)

## Changelog

<ChangeLog />
