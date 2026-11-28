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

import ChangeLog from '../changelog/connector-http-auth0.md';

# Auth0

> Auth0 Management API source · BATCH

## Description

Read application clients, users, organizations, organization members, and direct organization member role assignments from an Auth0 tenant. The connector uses one ordered tenant scan and shares each organization and member listing among the selected resources. A dedicated split is assigned to subtask zero. Use parallelism 1 for this source.

## Authentication and scopes

Set `tenant_url` to the fixed HTTPS tenant origin. For an existing Management API access token, configure `token`. For an authorized machine-to-machine application, configure `client_id`, `client_secret`, and `audience`. Use the audience for the tenant's Management API, for example `https://my-tenant.us.auth0.com/api/v2/`. The source renews an OAuth access token before expiry and attempts one renewal after a Management API 401. Tenant requests use the configured HTTPS origin and normal certificate and hostname verification. Enable `allow_custom_domain` explicitly for a trusted custom or private tenant hostname.

| Resource | Required permissions |
| --- | --- |
| `clients` | `read:clients` |
| `users` | `read:users` |
| `organizations` | `read:organizations` |
| `organization_members` | `read:organizations`, `read:organization_members` |
| `organization_member_roles` | `read:organizations`, `read:organization_members`, `read:organization_member_roles` |

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `tenant_url` | string | yes | | HTTPS origin without a path, user info, query, or fragment. |
| `allow_custom_domain` | boolean | no | `false` | Allow an explicitly configured nonstandard HTTPS tenant hostname. Standard Auth0 tenant domains end in `.auth0.com` and use HTTPS port 443. |
| `token` | string | conditional | | Management access token. Choose this or the OAuth client credential options. |
| `client_id` | string | conditional | | Machine-to-machine application identifier. |
| `client_secret` | string | conditional | | Machine-to-machine application secret. |
| `audience` | string | conditional | | HTTPS Management API identifier ending in `/api/v2/`. |
| `tls_trust_certificate` | string | no | | Connector-local X.509 trust anchor in PEM or base64 DER. |
| `resources` | list of string | no | all five | Resource names from the table above. An empty list selects all. |
| `start_date` | string | no | `2013-01-01T00:00:00.000Z` | Inclusive start of the users' `updated_at` range, as an ISO 8601 timestamp with offset and millisecond precision. |
| `end_date` | string | no | batch start time | Inclusive end of the users' range, in the same format. The initial boundary is frozen in the split. |
| `page_size` | int | no | `50` | Page size between 1 and 100. |
| `max_pages_per_resource` | int | no | `10000` | Logical request budget for each resource across the batch, including user count probes and all parents. |
| `connect_timeout_ms` | int | no | `10000` | Timeout for opening a TCP connection. |
| `request_timeout_ms` | int | no | `30000` | Deadline per HTTPS attempt. |
| `operation_timeout_ms` | int | no | `120000` | Deadline per logical read, including authentication, pacing and retries. |
| `min_request_interval_ms` | int | no | `100` | Minimum interval between attempts. |
| `max_retries` | int | no | `3` | Additional attempts for retryable read errors. |
| `max_response_bytes` | int | no | `4194304` | Limit on each encoded and decoded response body. |

### Output

The source emits `resource`, `id`, `organization_id`, `user_id`, and `record` as strings. The two context columns are null for clients, users, and organizations. Member rows contain both context columns and use the member's `user_id` as `id`. Role rows use the role ID and retain both parent identifiers. `record` preserves the fields returned by Auth0 as a JSON object. Those fields can contain personal data and application credentials depending on scope. Route the output to a destination authorized to store those fields.

### Pagination and recovery

Organization and member listings use Auth0 checkpoint pagination. Client and direct member role listings use offset pagination. The users search uses deterministic provider ordering by `updated_at`, inclusive millisecond ranges, and a count probe. A window at the 1,000-result threshold is divided before paging. A single millisecond still at the threshold fails explicitly. Auth0 user search is eventually consistent and applies its own search visibility and profile limits. The source provides a bounded read of the visible index, with no continuous updates or source snapshot isolation. [Auth0 user search limits](https://auth0.com/docs/manage-users/user-search/user-search-best-practices)

A checkpoint records the scan scope, search bounds, active parent IDs, page cursors, and emitted row position. On restore, an in-progress page is re-read and must match its previously committed digest. A static token may rotate within the same tenant. OAuth recovery keeps the same client ID and audience and permits a new client secret. Keep the permissions required by the selected resources. Mutable Auth0 data or an expired provider cursor may require starting a fresh scan.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Auth0 {
    tenant_url = "https://my-tenant.us.auth0.com"
    token = ${AUTH0_TOKEN}
    resources = ["clients", "users", "organizations", "organization_members", "organization_member_roles"]
    start_date = "2025-06-01T00:00:00.000Z"
  }
}

sink {
  LocalFile {
    path = "/secure/auth0-export"
    file_format_type = "json"
  }
}
```

<ChangeLog />
