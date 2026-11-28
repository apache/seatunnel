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

import ChangeLog from '../changelog/connector-http-basecamp.md';

# Basecamp

> Basecamp source · BATCH

## Description

Read active projects, schedule tools, schedule entries, todos and messages from one Basecamp account. The connector traverses the project tree before reading the todo and message feeds. A child selection still discovers its parent projects and tools. The scan runs on subtask zero. Use parallelism 1 for this source.

| Resource | Read behavior |
| --- | --- |
| `projects` | List visible active projects. |
| `schedules` | Select schedule tools from each project detail's dock. |
| `schedule_entries` | List entries for each enabled schedule. |
| `todos` | Read account-wide Todo recordings in descending update order. |
| `messages` | Read account-wide Message recordings in descending update order. |

A disabled schedule is emitted if `schedules` is selected. Only enabled schedules have their entries requested. Requests returning a missing resource or denied access fail with the HTTP status and resource context.

## Authentication

Use an existing Basecamp bearer access token or a registered Launchpad integration's client credentials and refresh token. Configure `user_agent` with your application name and contact so Basecamp can identify the integration. Data requests use `https://3.basecampapi.com` and the selected account path. The refresh grant is submitted to `https://launchpad.37signals.com`. The connector tries one refresh after a data 401 when using Launchpad credentials. [Basecamp authentication](https://github.com/basecamp/bc-api/blob/master/sections/authentication.md)

A refresh token returned by Launchpad replaces the previous value in the current reader's memory. The connector does not store credentials in savepoints or write them to a configuration file. Supply a currently valid refresh credential when restarting a job, especially if your authorization server invalidates an older token. An existing bearer token is used until it expires or is revoked.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `account_id` | string | yes | | Decimal Basecamp account identifier. |
| `user_agent` | string | yes | | Identifiable application name and contact. |
| `token` | string | conditional | | Existing bearer token. Choose this or the Launchpad refresh options. |
| `client_id` | string | conditional | | Launchpad integration identifier. |
| `client_secret` | string | conditional | | Launchpad integration secret. |
| `refresh_token` | string | conditional | | Current Launchpad refresh token. |
| `tls_trust_certificate` | string | no | | Optional connector-local X.509 trust anchor in PEM or base64 DER. Hostname verification still applies. |
| `resources` | list of string | no | all five | Resource names from the table above. An empty list selects all. |
| `start_date` | string | for todos or messages | | Inclusive lower bound for recording updates. Use an ISO 8601 timestamp with offset. |
| `end_date` | string | no | batch start time | Inclusive upper bound for recording updates. A supplied value can shorten the default window. |
| `max_pages_per_collection` | int | no | `10000` | Page limit for the projects list, either recording feed, or one schedule's entries. |
| `max_page_items` | int | no | `1000` | Maximum accepted records in one page or tools in one project dock. |
| `connect_timeout_ms` | int | no | `10000` | TCP connect timeout. |
| `request_timeout_ms` | int | no | `30000` | Deadline for one HTTPS attempt. |
| `operation_timeout_ms` | int | no | `120000` | Combined deadline across authentication and retries for one read. |
| `min_request_interval_ms` | int | no | `100` | Minimum interval between attempts. |
| `max_retries` | int | no | `3` | Additional attempts after transient errors. |
| `max_response_bytes` | int | no | `4194304` | Limit for encoded and decoded response bytes. |

## Output and recovery

The source emits `resource`, `id`, `project_id`, `schedule_id`, `updated_at`, and `record` as strings. IDs remain exact. The project and schedule context fields are included when applicable. For recordings the project ID comes from the recording's bucket, when present. The `record` column preserves the provider's JSON object, including formatted text and nested fields. On schedule rows it adds numeric `project_id` and `substream_data` fields derived from the project and schedule IDs.

The recording bounds are frozen for the batch. Equal update timestamps are retained across pages. A savepoint tracks the exact resource, parent IDs, validated Link query and accepted row. When restored, the reader validates the active page and parent schedule dock before emitting the remaining rows. The provider does not expose a transactional snapshot or deletion history through these read endpoints. A changed active page can require starting a new bounded scan. [Basecamp API pagination](https://github.com/basecamp/bc-api#pagination)

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Basecamp {
    account_id = "123456"
    user_agent = "Data export (integrations@example.com)"
    token = ${BASECAMP_ACCESS_TOKEN}
    start_date = "2025-06-01T00:00:00.000Z"
    resources = ["projects", "schedules", "schedule_entries", "todos", "messages"]
  }
}

sink {
  LocalFile {
    path = "/data/basecamp-export"
    file_format_type = "json"
  }
}
```

<ChangeLog />
