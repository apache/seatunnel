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

import ChangeLog from '../changelog/connector-http-calendly.md';

# Calendly

Install `connector-http-calendly` to export Calendly API v2 records in batch mode. Configure one resource per source block and set source parallelism to `1`. The source checks the authenticated user and the current organization. Grant `users:read` and the read scope for the selected resource and any parent collections. Organization-wide reads may require an owner or admin, and some resources depend on subscription or feature access. [Calendly API reference](https://developer.calendly.com/api-docs/overview/api/reference)

## Resources

| `resource` | Records |
| --- | --- |
| `api_user` | Current authenticated user |
| `event_types` | Event types in the current organization |
| `groups` | Groups in the current organization |
| `organization_memberships` | Organization members |
| `scheduled_events` | Scheduled and canceled events |
| `event_invitees` | Invitees of each discovered event |
| `teams` | Teams visible to the caller |
| `event_type_memberships` | Members of each event type |
| `event_type_availability_schedules` | Named schedules or custom hours for each event type |
| `group_relationships` | Relationships of each group |
| `user_availability_schedules` | Availability schedules for each organization member |
| `locations` | Meeting locations for each organization member |
| `organization_invitations` | Invitations in the current organization |
| `webhook_subscriptions` | Organization-scoped webhook subscriptions |
| `routing_forms` | Routing forms in the current organization |
| `routing_form_submissions` | Submissions of each routing form |
| `contacts` | Contacts visible to the caller |
| `outgoing_communications` | Organization SMS and email records |
| `activity_log_entries` | Organization activity log, subject to Enterprise access |
| `meeting_recaps` | Available Notetaker meeting recaps visible to the caller |

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `resource` | string | Required | One resource from the table. |
| `token` | string | PAT authentication | Personal access token. Supply this or the three OAuth settings. |
| `client_id` | string | OAuth authentication | Calendly OAuth application client ID. |
| `client_secret` | string | OAuth authentication | Calendly OAuth application client secret. |
| `oauth_token_file` | string | OAuth authentication | Absolute path to the private writable refresh state on each eligible worker. |
| `start_date` | string | `1970-01-01T00:00:00Z` | RFC 3339 lower bound for the date windows described below. |
| `end_date` | string | First read time | RFC 3339 upper cursor bound, fixed across checkpoint restoration. |
| `incremental_start` | string | Unset | RFC 3339 high-water mark from a previous export of one of the six cursor resources. |
| `lookback_days` | int | `0` | Extra replay days for `scheduled_events` after an incremental export, from 0 to 3650. |
| `page_size` | int | `100` | Rows requested on counted endpoints, from 1 to 100. |
| `max_pages` | int | `2000` | Total accepted collection pages, including hidden parent and empty pages, from 1 to 20000. |
| `connect_timeout_ms` | int | `10000` | TCP connect timeout in milliseconds, from 1 to 120000. |
| `read_timeout_ms` | int | `30000` | Socket read timeout in milliseconds, from 1 to 120000. |
| `request_deadline_ms` | int | `90000` | Total API read, OAuth exchange and retry budget in milliseconds, from 1 to 300000. |
| `max_attempts` | int | `4` | Maximum GET attempts, including the first and any OAuth recovery replay, from 1 to 10. |
| `max_response_bytes` | int | `8388608` | Maximum encoded and decoded size per response in bytes, from 1024 to 33554432. |
| `tls_trust_certificate` | string | JVM trust store | Optional absolute path to local PEM or DER X.509 trust anchors. Hostname verification always applies. |

## Authentication

Use one authentication method. For a PAT, set `token`. For OAuth, create a Calendly OAuth application and complete authorization outside SeaTunnel. Provision the resulting refresh token as a UTF-8 JSON file with mode `0600` before starting the job:

```json
{"version":1,"refresh_token":"REPLACE_WITH_GRANTED_REFRESH_TOKEN"}
```

Set `client_id`, `client_secret` and `oauth_token_file`. Mount the same authoritative durable file and directory at the configured absolute path on every Zeta worker eligible to run or restore this source. Use a POSIX filesystem that supports advisory file locking, atomic rename and file and directory synchronization. The job user must own the token file and directory. The token and lock files must be regular files with a single link. The directory must be writable by that user and have group and other write access disabled. The source creates a private sibling `.lock` file. Keep one source deployment per token store and stop it before provisioning a replacement token.

Every successful refresh persists the new refresh token before using the access token. The source passes Basic credentials only to `auth.calendly.com` and Bearer credentials only to `api.calendly.com`. A rejected or unconfirmed token POST leaves a durable pending marker and requires reauthorization and replacement of the token file. On a GET returning `401`, the OAuth source can refresh once and repeat the same GET within its budget. [Calendly refresh token rotation](https://developer.calendly.com/docs/authentication/refresh-token-rotation-guide)

## Date windows

| Resource | Cursor field | Initial floor | API bounds and replay |
| --- | --- | --- | --- |
| `event_types` | `updated_at` | 1970-01-01 | None |
| `scheduled_events` | `updated_at` | 1970-01-01 | `min_start_time` uses `start_date`; replay adds five minutes and `lookback_days` |
| `event_invitees` | `created_at` | `start_date` | Hidden event discovery includes events started before `start_date` |
| `outgoing_communications` | `created_at` | `start_date` | `min_created_at` and `max_created_at` |
| `activity_log_entries` | `occurred_at` | `start_date` | `min_occurred_at` and `max_occurred_at` |
| `meeting_recaps` | `start_time` | `start_date` | `start_time` and `end_time`; three-day replay overlap |

On a fresh run, `incremental_start` can accept the last cursor high-water mark saved by the downstream job. The source clamps the replay floor to the initial floor. Every returned row is checked against the inclusive local cursor window. Server-supplied time filters also apply to rows available for this check. In particular, a meeting that starts before the configured `start_date` is outside the `scheduled_events` export even when updated later. Invitee discovery scans the parent events without that start-time filter. The source reads every continuation even when a page contains older timestamps. A saved or repeated overlap can intentionally emit a record again.

## Output and recovery

| String column | Value |
| --- | --- |
| `id` | Last path segment of the record's canonical Calendly API URI when present. Availability schedules use the named rule URI. Custom hours, locations and communications can have a null ID. |
| `parent_id` | ID of the request scope, such as the organization, event type, user, group, event or routing form. Null for responses without a request scope. |
| `updated_at` | Normalized UTC cursor timestamp for the six date-window resources. Other rows keep their original `updated_at` when present. |
| `data` | Full JSON object with the original numbers, nested values and nulls. The source adds `id` to events and invitees, `scheduled_event_id` to invitees, `user` to locations, and `availability_rule_uri` to event type availability schedules. |

Pagination uses the opaque `next_page_token`, including after empty pages. Response URLs remain data. Connections use TLS, verify the configured API hostname and reject redirects. The source retries safe GET responses within the configured limits, respecting `Retry-After` or `X-RateLimit-Reset` on throttling. A discovered child that disappears before its first page can be skipped. Permission or plan errors fail the job, including general `403` responses. [Calendly API rate limits](https://developer.calendly.com/api-docs/overview/rate-limits)

For a checkpoint restore, the source verifies the saved user and organization identity, then replays the active parent and child pages. A saved partial page must still match its original records and continuation before the next offset can be emitted. A terminal split finishes without authentication or API reads. The API can change between pages, so a batch run can observe updates while it reads. Saved offsets restore progress within the same job. Across independent jobs, pass a downstream high-water mark explicitly.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Calendly {
    resource = "scheduled_events"
    token = ${CALENDLY_PERSONAL_ACCESS_TOKEN}
    start_date = "2025-01-01T00:00:00Z"
  }
}
sink {
  Console {}
}
```

<ChangeLog />
