import ChangeLog from '../changelog/connector-http-open-data-dc.md';

# Open Data DC

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
## Description

`OpenDataDC` reads a fixed tabular projection of the public [Crashes in DC](https://catalog.data.gov/dataset/crashes-in-dc)
ArcGIS layer from the District of Columbia. It requires no credentials and makes read-only HTTPS requests.
The source supports bounded batch jobs with source parallelism 1. Geometry, related tables and CDC are not supported.

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `page_size` | Integer | `500` | Rows to request per page, from 1 to 1000. |
| `max_rows` | Long | `0` | Stop after this many rows, or scan the full range if zero. |
| `query_url` | String | City Crashes layer | ArcGIS-compatible Crashes `/query` URL for a trusted mirror or local fixture; no user info, query string or fragment. |
| `allow_insecure_http` | Boolean | `false` | Opt in to plaintext HTTP for a trusted local fixture only. |

The output table is named `crashes`.

| Column | SeaTunnel type | Nullable |
| --- | --- | --- |
| OBJECTID | BIGINT | No |
| CCN | STRING | Yes |
| REPORTDATE | TIMESTAMP (UTC) | Yes |
| WARD | STRING | Yes |
| TOTAL_VEHICLES | INT | Yes |
| LATITUDE | DOUBLE | Yes |
| LONGITUDE | DOUBLE | Yes |

The source captures the highest available `OBJECTID` at scan start and reads in ascending key order. If MapServer reports an
empty page with more results available, it advances the offset by a full requested window for that same query.
After a nonempty page it resumes keyset pagination. The source fails rather than silently truncating after 32
additional empty windows.
Checkpoint state records the scan bound, last emitted key and row count. This provides a bounded scan, not a transactional
snapshot: published rows can be updated or removed during a run, and the City may regenerate object IDs between runs.
Treat `OBJECTID` as the pagination key for one scan, not a durable business identifier.
If an HTTP error triggers a job retry without a recoverable checkpoint, earlier rows can be replayed. Configure an idempotent sink if duplicates are unacceptable.

HTTP 429 retries only the same GET, at most twice. A valid `Retry-After` up to five seconds is honored; a longer
delay fails rather than retrying early. The retry budget is per request and does not change the job-level replay behavior.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  OpenDataDC {
    page_size = 2
    max_rows = 4
  }
}
sink {
  Console {}
}
```

## Changelog

<ChangeLog />
