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

import ChangeLog from "../changelog/connector-http-buildkite.md";

# Buildkite

## Description

The Buildkite source runs a bounded batch over twelve REST API resources. It sends HTTPS GET requests to `api.buildkite.com` using an API access token. Select read scopes for the resources you need. Organizations without an active plan are omitted by the API. A scoped request that receives HTTP 404 fails the job. [Buildkite REST API](https://buildkite.com/docs/apis/rest-api) and [API access tokens](https://buildkite.com/docs/apis/managing-api-tokens).

| Resource | Direct read scope | Record key | Creation-time filter |
| --- | --- | --- | --- |
| `organizations` | `read_organizations` | `id` | Local |
| `analytics_organizations_suites` | `read_suites` | `id` | |
| `organizations_pipelines` | `read_pipelines` | `id` | Local |
| `access-token` | | `uuid` | |
| `builds` | `read_builds` | `id` | API and local |
| `organizations_clusters` | `read_clusters` | `id` | Local |
| `organizations_builds` | `read_builds` | `id` | API and local |
| `organizations_pipelines_builds` | `read_builds` | `id` | API and local |
| `organizations_clusters_queues` | `read_clusters` | `id` | Local |
| `organizations_clusters_tokens` | `read_clusters` | `id` | Local |
| `organizations_emojis` | | `name` | |
| `user` | `read_user` | `id` | Local |

A single reader visits each organization, then its selected subcollections. It discovers pipeline builds under their own pipeline and queues and agent tokens under their own cluster. Selecting an organization child also requires `read_organizations` for discovery. Pipeline builds require `read_pipelines`, and agent token metadata can require cluster management permission. Token and agent token endpoints return metadata. The source discards any unexpected secret credential fields in token, agent token and suite metadata. [Organizations API](https://buildkite.com/docs/apis/rest-api/organizations), [access token API](https://buildkite.com/docs/apis/rest-api/access-token) and [agent tokens](https://buildkite.com/docs/apis/rest-api/clusters/agent-tokens).

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| token | string | yes | | Buildkite API access token. |
| resources | list | no | [] | Resource names from the table. An empty list selects all twelve. |
| start_date | string | for a dated resource | | Inclusive creation time with an RFC 3339 offset. |
| end_date | string | no | Batch startup | Exclusive creation cutoff, frozen before reading. |
| page_size | int | no | 100 | Items requested per standard page, from 1 to 100. |
| max_pages_per_collection | int | no | 10000 | Maximum accepted pages per collection under one parent. |
| max_pages_per_scan | int | no | 100000 | Maximum accepted responses across the scan. |
| max_unpaginated_items | int | no | 10000 | Maximum records in a full-list response. |
| tls_trust_certificate | string | no | JVM default trust | Optional source-local PEM or base64 DER X.509 trust anchor. Hostname verification stays enabled. |
| connect_timeout_ms | int | no | 10000 | TCP connection timeout in milliseconds. |
| request_timeout_ms | int | no | 30000 | Deadline for one HTTPS attempt. |
| operation_timeout_ms | int | no | 120000 | Deadline across one read's attempts and waits. Must be at least `request_timeout_ms`. |
| request_interval_ms | int | no | 100 | Minimum spacing between attempts in milliseconds. |
| max_retries | int | no | 3 | Additional attempts after a transient read error. |
| max_retry_delay_ms | int | no | 60000 | Maximum permitted retry delay, including a server reset or Retry-After hint. |
| max_response_bytes | int | no | 8388608 | Maximum response bytes before and after decompression. |

Standard lists follow the next relation from the HTTP `Link` header until it is absent. Empty and short pages can have a next link. The reader validates the collection and reconstructs its original filters for every request. Each organization emoji list is read in one request. The suites endpoint supports a full response without a next link, or continued pages when Buildkite supplies a next link. `access-token` and `user` return single objects. [Pagination](https://buildkite.com/docs/apis/rest-api#pagination), [suites](https://buildkite.com/docs/apis/rest-api/test-engine/suites) and [emojis](https://buildkite.com/docs/apis/rest-api/emojis).

A transient read can be retried within the configured limits. When Buildkite throttles a request, the source uses the applicable organization, user and deep-builds reset hints. A delay that exceeds the retry or operation limit fails the read. Malformed responses, redirects, and authentication or permission errors fail the job. [Buildkite rate limits](https://buildkite.com/docs/apis/rest-api/rate-limits).

## Time bounds and checkpoints

Dated records use UTC creation times in the half-open interval `[start_date, end_date)`. Only build requests send `created_from` and `created_to`. The other dated lists are filtered after each response. A parent created before the interval can still lead to qualifying descendants. Builds are filtered by their creation times, so a later state change to an older build requires a subsequent read covering that build's creation time. If Buildkite rejects a deep build listing, use shorter time intervals. [Builds API](https://buildkite.com/docs/apis/rest-api/builds).

The checkpoint keeps the original cutoff and the current stack of organization, parent and child positions. On restore, the source verifies the saved page digests and expected parent paths before resuming. Completed rows and collections are skipped. Changing the credential or selection requires a new batch. Buildkite can change a collection during pagination. A changed replayed page fails the restore, and a new extraction can read the same records.

## Output

All seven columns use string types.

| Column | Meaning |
| --- | --- |
| `resource` | Selected resource name. |
| `id` | Native record key. |
| `organization_slug` | Organization slug for its own record and descendants. |
| `pipeline_slug` | Pipeline slug for pipeline records and their build lists. |
| `cluster_id` | Cluster ID for cluster records, queues and agent token lists. |
| `created_at` | Normalized UTC creation time for a dated resource, or null. |
| `record` | JSON record with provider fields, nested values, nulls and exact numbers. |

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Buildkite {
    token = ${BUILDKITE_TOKEN}
    resources = ["organizations_pipelines_builds", "organizations_emojis"]
    start_date = "2025-01-01T00:00:00Z"
    end_date = "2025-02-01T00:00:00Z"
  }
}
```

## Changelog

<ChangeLog />
