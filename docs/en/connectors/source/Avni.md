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

import ChangeLog from '../changelog/connector-http-avni.md';

# Avni

> Avni integration API source · BATCH

## Description

The source reads subjects, program enrolments, program encounters and general encounters. It performs a bounded scan over the selected resources in that order. Each resource is paged separately. Its records retain the provider's related IDs, observations and voided flags. The scan runs on subtask zero. Use parallelism 1 for this source.

| Resource | Avni integration endpoint |
| --- | --- |
| `subjects` | `/api/subjects` |
| `program_enrolments` | `/api/programEnrolments` |
| `program_encounters` | `/api/programEncounters` |
| `encounters` | `/api/encounters` |

## Authentication

Use an Avni integration account with the User role and permission to view the selected resources. The account needs to support unattended AWS Cognito user-password sign-in. A password change, MFA or other sign-in challenge must be resolved before use. The connector discovers the public Cognito client ID from Avni, exchanges credentials at the separate Cognito origin and sends the resulting ID token to Avni in the `auth-token` header. One new sign-in is attempted after a data request returns 401. [Avni API guide](https://avni.readme.io/docs/api-guide)

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `username` | string | yes | | Avni integration account username. |
| `password` | string | yes | | Avni integration account password. |
| `start_date` | string | yes | | Exclusive lower bound for audit timestamps as an ISO 8601 timestamp with offset and millisecond precision. |
| `end_date` | string | no | batch start minus 10 seconds | Exclusive upper bound. An explicitly supplied time can shorten the default window. |
| `api_origin` | string | no | `https://app.avniproject.org` | Fixed Avni HTTPS origin. |
| `identity_origin` | string | no | `https://cognito-idp.ap-south-1.amazonaws.com` | Fixed Cognito HTTPS origin, distinct from the Avni origin. |
| `allow_custom_endpoints` | boolean | no | `false` | Allow explicitly trusted nonstandard HTTPS origins. |
| `tls_trust_certificate` | string | no | | Optional connector-local X.509 trust anchor in PEM or base64 DER. Both origins still require hostname verification. |
| `resources` | list of string | no | all four | Select resource names from the table above. An empty list selects all. |
| `page_size` | int | no | `100` | Requested page size, from 1 to 1,000. |
| `max_pages_per_resource` | int | no | `10000` | Maximum pages accepted for one resource. |
| `connect_timeout_ms` | int | no | `10000` | TCP connect timeout. |
| `request_timeout_ms` | int | no | `30000` | Deadline per HTTPS attempt. |
| `operation_timeout_ms` | int | no | `120000` | Deadline across discovery, login, retries and one data read. |
| `min_request_interval_ms` | int | no | `100` | Minimum interval between HTTPS attempts. |
| `max_retries` | int | no | `3` | Additional attempts for retryable errors. |
| `max_response_bytes` | int | no | `4194304` | Limit for both encoded and decoded body bytes. |

## Output and recovery

Each row contains `resource`, `id`, `last_modified_at` and `record` as strings. The ID comes from `ID`. The source converts `audit["Last modified at"]` to UTC and keeps the original JSON object in `record`. The JSON can contain personal or clinical data. Use a destination authorized to store the selected records.

All pages use the same exclusive time bounds. The connector preserves page and row positions across checkpoints, including when many records have the same audit timestamp. It verifies the page fingerprint on restore before releasing any remaining rows. Changes to a collection or expired upstream data may require a fresh bounded scan. Avni returns currently visible records and the connector observes updates in their audit order. This interface supplies no transaction snapshot, deletion log or continuous CDC.

For consecutive batch scans, choose overlapping time windows with ID-based reconciliation if records can arrive late. Exact adjacent exclusive windows exclude records precisely at their shared boundary.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Avni {
    username = ${AVNI_USERNAME}
    password = ${AVNI_PASSWORD}
    start_date = "2025-06-01T00:00:00.000Z"
    resources = ["subjects", "program_enrolments", "program_encounters", "encounters"]
  }
}

sink {
  LocalFile {
    path = "/secure/avni-export"
    file_format_type = "json"
  }
}
```

<ChangeLog />
