import ChangeLog from '../changelog/connector-http-matomo.md';

# Matomo

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

The Matomo source reads daily `VisitsSummary.get` data for one website through the
[Matomo Reporting API](https://developer.matomo.org/guides/reporting-api). It is a
bounded batch source. It does not read individual visits, support segments, or persist
an incremental cursor.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `url` | String | Yes | — | HTTP(S) Reports API endpoint. Include the installation path and `index.php` when the server needs it. Do not include credentials, query parameters, or a fragment. |
| `site_id` | Integer | Yes | — | Positive Matomo website ID. |
| `start_date` | String | Yes | — | First report day, inclusive, formatted as `YYYY-MM-DD`. |
| `end_date` | String | Yes | — | Last report day, inclusive. The interval can contain 1–366 days. |
| `token` | String | No | — | A Matomo token with View access to the requested site. If omitted, sends an anonymous GET. When configured, requires HTTPS and sends it in a POST form body, never in the URL. |
| `timeout_ms` | Integer | No | 15000 | Connect and socket read timeout, 1–120000 milliseconds. |

Set job mode to `BATCH` and source parallelism to one. The source does not follow
redirects. When a request fails, the job fails instead of treating missing or
invalid data as an empty day. A day explicitly returned as an empty array is not
emitted. Dates are the reporting days defined by the site's Matomo time zone.
Repeating the same range can emit the same dates again; the destination can use
`(site_id, report_date)` as its reconciliation key.

`timeout_ms` retains its connect and socket-inactivity meaning. An independent fixed 120-second
elapsed ceiling covers HTTP fetching and complete response validation, including a body that
continues to make progress. Responses are limited to 2 MiB after decompression, 100 headers and
16 KiB per status/header line. Invalid JSON or trailing content fails before any report row is
emitted. HTTP proxy CONNECT refusal bodies also have a separate 2 MiB encoded-byte limit before
native buffering or authentication draining. The request is aborted on timeout; platform DNS
resolution and native authentication-provider work retain their platform cancellation behavior.
The elapsed check prevents accepting a late result; it does not forcibly terminate those providers.

The complete report interval is validated before output. The reader keeps that response locally
and accepts at most one row per poll, allowing the engine's cooperative checkpoint handoff
between rows without fetching again. A single downstream collection can still wait; this is not
a wall-clock checkpoint guarantee. Checkpoints retain the existing null split, not the prepared
response or row position. A restored reader fetches and replays the entire current interval, so
previously emitted days can repeat or change. This does not provide exactly-once delivery.

## Output

| Field | Type | Nullable |
| --- | --- | --- |
| `site_id` | INT | No |
| `report_date` | STRING | No |
| `visits` | BIGINT | No |
| `actions` | BIGINT | No |
| `unique_visitors` | BIGINT | Yes |

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Matomo {
    url = "https://demo.matomo.cloud/"
    site_id = 1
    start_date = "2026-06-01"
    end_date = "2026-06-02"
  }
}
sink {
  Console {}
}
```

The example uses the vendor's public demo and needs no token. Demo data is not
a fixed fixture. For a private site, supply the token using your deployment's
secret configuration, not in a checked-in file.

<ChangeLog />
