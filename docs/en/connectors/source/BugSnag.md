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

# BugSnag Source

`BugSnag` reads the v2 Data Access API in a bounded scan at source parallelism 1. Install `connector-http-bugsnag` if the artifact is absent from your distribution.

Create a Personal Auth Token in **My account → Personal auth tokens**. The connector sends `Authorization: token <token>` and `X-Version: 2` on every request. Access depends on the token owner's organization and project permissions and the BugSnag plan. [Data Access API](https://developer.smartbear.com/bugsnag/docs/bugsnag-data-access-api) · [Authentication](https://developer.smartbear.com/bugsnag/docs/data-access-authentication)

## Resources

| Resource | API route | Output time |
| --- | --- | --- |
| `organizations` | `/user/organizations` | |
| `projects` | `/organizations/{organization_id}/projects` | `created_at` |
| `saved_searches` | `/projects/{project_id}/saved_searches` | |
| `saved_searches_usage_summary` | `/saved_searches/{search_id}/usage_summary` | |
| `errors` | `/projects/{project_id}/errors` | `last_seen` |
| `events` | `/projects/{project_id}/events` | `received_at` |
| `pivots` | `/projects/{project_id}/pivots` | |
| `supported_integrations` | `/integrations` | |
| `collaborators` | `/organizations/{organization_id}/collaborators` | |
| `teams` | `/organizations/{organization_id}/teams` | |
| `event_fields` | `/projects/{project_id}/event_fields` | |
| `releases` | `/projects/{project_id}/releases` | `release_time` |
| `trace_fields` | `/projects/{project_id}/trace_fields` | |

The connector discovers organizations, projects and saved searches needed by selected descendants, even when those parent rows are unselected or outside the configured time window. Events request `full_reports=true` as a query parameter. Each saved search usage summary is one object and retains the discovered search and project IDs.

## Options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `token` | Yes | | Personal Auth Token without the `token` prefix. |
| `resources` | Yes | | Non-empty list of unique resource names from the table. |
| `start_date` | Yes | | Inclusive RFC 3339 lower bound for dated output. |
| `end_date` | No | Scan start | Exclusive RFC 3339 upper bound, frozen in the checkpoint. |
| `page_size` | No | 50 | Request size for operations with `per_page`, from 1 to 100. Releases use up to 10. |
| `max_pages_per_scope` | No | 1000 | Maximum pages per resource and parent, from 1 to 10000. |
| `max_requests` | No | 100000 | Maximum successfully processed responses in the scan, from 1 to 1000000. |
| `max_attempts` | No | 4 | Total attempts per request, from 1 to 10. |
| `request_timeout_ms` | No | 30000 | Per-request acquisition deadline including retries and decoding, from 100 to 300000 milliseconds. |
| `max_response_bytes` | No | 8388608 | Decoded response limit, from 1024 to 67108864 bytes. |
| `endpoint` | No | `https://api.bugsnag.com` | Bare HTTPS origin. Use `https://api.bugsnag.smartbear.com` for the corresponding dashboard or the configured on-premise API origin. |
| `trust_certificate` | No | | Worker-local X.509 PEM. Required for custom HTTPS origins. |

## Output

The `bugsnag.records` table contains seven `STRING` columns.

| Column | Description |
| --- | --- |
| `resource` | Resource name. |
| `record_id` | Provider `id`, integration `key`, pivot `event_field_display_id`, field `display_id`, or saved search ID for a usage summary. |
| `organization_id` | Discovered organization ID, when applicable. |
| `project_id` | Discovered project ID, when applicable. Collaborators and teams are scoped by `organization_id`. |
| `search_id` | Saved search ID for searches and their summaries. |
| `record_time` | Relevant provider timestamp normalized to UTC, or null for undated resources. |
| `payload` | Original provider object represented as JSON text, preserving nested, extra and null values and numerical precision. |

A record key belongs to its resource and discovered parent scope. Payloads can contain project or organization API keys, user data and full event details returned by BugSnag.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  BugSnag {
    token = ${BUGSNAG_AUTH_TOKEN}
    resources = ["projects", "errors", "events", "releases"]
    start_date = "2025-01-01T00:00:00Z"
    end_date = "2025-02-01T00:00:00Z"
  }
}

sink {
  Console {}
}
```

## Paging, time bounds and recovery

The connector follows an advertised `Link` relation of `next` through short and empty pages. It accepts offsets only for the original HTTPS origin, API route and fixed request parameters. For releases, the documented numeric offset advances by the raw response count when no next link is supplied. Release requests use the frozen scan end as their `base`. A list without a requested page size allows up to 10,000 records in one response. [Paging](https://developer.smartbear.com/bugsnag/docs/data-access-pagination)

For the four dated resources, a row is emitted when its provider timestamp falls in `[start_date, end_date)`. The scan traverses available provider pages without using the output window to stop pagination or exclude parents. The available data depends on provider retention and permissions. BugSnag Data Access does not expose a point-in-time snapshot or a continuous CDC feed through these operations.

State retains the frozen end, pending ancestry, validated offsets and issued row counts. Recovery compares the current page's records and continuation with its checkpoint fingerprint before publishing the remaining rows. A new token, resource set, date window, origin, certificate path or page size requires a fresh scan. The HTTPS client verifies certificates and hostnames, forbids redirects and bounds retries within the request deadline. HTTP 429 requires a usable `Retry-After`. [Rate limits](https://developer.smartbear.com/bugsnag/docs/data-access-rate-limiting)
