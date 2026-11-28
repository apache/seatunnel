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

# Buzzsprout Source

`Buzzsprout` reads podcast and episode metadata in a bounded scan at source parallelism 1. Install `connector-http-buzzsprout` if the artifact is absent from your distribution.

| Resource | Operation | Scope |
| --- | --- | --- |
| `podcasts` | `GET /api/podcasts` | All podcasts accessible to the token owner. |
| `episodes` | `GET /api/{podcast_id}/episodes` | Episodes in the configured podcast. |

Create the API token in **Profile → API**. The source sends `Authorization: Token token=<token>` and an identifiable application user agent. It uses the documented extensionless endpoints. URLs in podcast and episode records are retained as data. [Buzzsprout API](https://github.com/buzzsprout/buzzsprout-api) · [Podcasts](https://github.com/buzzsprout/buzzsprout-api/blob/master/sections/podcasts.md) · [Episodes](https://github.com/buzzsprout/buzzsprout-api/blob/master/sections/episodes.md)

## Options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `token` | Yes | | API token value from the profile. |
| `resources` | Yes | | Non-empty ordered list of unique resource names. |
| `podcast_id` | For `episodes` | | Positive decimal podcast ID supplied as a string. |
| `start_date` | For `episodes` | | Inclusive episode publication time, an RFC 3339 timestamp with time zone. |
| `end_date` | No | Scan start | Exclusive episode publication time in the same format. The scan freezes this instant. |
| `include_undated` | No | false | Include episodes whose `published_at` is missing or null. |
| `max_attempts` | No | 4 | Total HTTPS attempts per list, from 1 to 10. |
| `request_timeout_ms` | No | 30000 | Total acquisition deadline per list including retries and parsing, from 100 to 300000 milliseconds. |
| `max_response_bytes` | No | 8388608 | Decoded response byte limit, from 1024 to 67108864. |
| `endpoint` | No | `https://www.buzzsprout.com` | Optional bare HTTPS origin paired with `trust_certificate`. |
| `trust_certificate` | No | | Worker-local X.509 PEM path for the custom origin. |

## Output

The `buzzsprout.records` table contains five `STRING` columns.

| Column | Description |
| --- | --- |
| `resource` | `podcasts` or `episodes`. |
| `record_id` | Positive provider ID represented as exact decimal text. |
| `podcast_id` | Podcast ID, taken from the podcast record or the configured episode scope. |
| `published_at` | Episode publication timestamp normalized to UTC. Null for podcast rows and undated episodes. |
| `payload` | Full provider object as JSON text, retaining nested fields, nulls and numeric precision. |

The episode filter includes timestamps in `[start_date, end_date)` regardless of the visibility or processing state. A stored publication timestamp may refer to a private or future episode. The window can include scheduled records when an explicit end covers them. The payload retains visibility, inactivity, media and contact fields when provided.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Buzzsprout {
    token = ${BUZZSPROUT_API_TOKEN}
    resources = ["podcasts", "episodes"]
    podcast_id = "140447"
    start_date = "2026-01-01T00:00:00Z"
    end_date = "2026-04-01T00:00:00Z"
  }
}

sink {
  Console {}
}
```

## List bounds and recovery

Both list endpoints provide a root JSON array. The source validates the complete array before emitting rows, with a 50,000-record ceiling and the configured byte limit. Negotiated gzip is decoded before applying the byte limit. An unexpected continuation header fails the list.

A checkpoint records the frozen upper time bound, resource position, raw array fingerprint and published row count. Partial recovery re-reads the same array and checks its content before publishing the remaining rows. It also checks records filtered by publication time. Changed token, resource order, podcast ID, time window, undated-row selection, origin or trust path requires a fresh scan. The source can resume a stable response. The provider supplies no isolated multi-resource snapshot or delete stream through these operations.

The HTTPS client validates TLS and hostname, forbids redirects and retries transient GET failures within the configured deadline. HTTP 429 uses bounded exponential backoff and respects a valid `Retry-After` lower bound, including fractional seconds. The two-list scan makes at most 20 attempts. Buzzsprout applies its 60-requests-per-minute quota to all callers sharing the authorization value. [Rate limits and retries](https://github.com/buzzsprout/buzzsprout-api#rate-limits-and-retries)
