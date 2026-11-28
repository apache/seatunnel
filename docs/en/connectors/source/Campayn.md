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

# Campayn Source

`Campayn` reads the Campayn v1 API in a bounded scan of one tenant at source parallelism 1. Install `connector-http-campayn` if it is absent from your distribution.

| Resource | Operation |
| --- | --- |
| `lists` | `GET /api/v1/lists.json` |
| `forms` | `GET /api/v1/lists/{list_id}/forms.json` |
| `contacts` | `GET /api/v1/lists/{list_id}/contacts.json` |
| `emails` | `GET /api/v1/emails.json` |
| `reports` | `GET /api/v1/reports/calendar.json` |

The API key comes from the Campayn account. Each GET uses `Authorization: TRUEREST apikey=<token>`. The `sub_domain` value sets the tenant's HTTPS host. Forms and contacts discover all accessible list IDs, including when `lists` is unselected for output. Each child record carries its real parent list ID. The calendar read includes sent and scheduled records for all provider dates. Scheduled reports can contain a null `report_url`. Forms' HTML, preview links and report links are preserved as data. [Campayn API](https://campayn.com/api) · [API reference](https://github.com/nebojsac/Campayn-API) · [Calendar report](https://github.com/nebojsac/Campayn-API/blob/master/endpoints/reports.md)

## Options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `token` | Yes | | Campayn API key without the authorization prefix. |
| `sub_domain` | Yes | | One DNS label under `campayn.com`. Use up to 63 ASCII letters, digits or hyphens, beginning and ending with a letter or digit. |
| `resources` | Yes | | Non-empty list of unique resource names from the table. |
| `max_lists` | No | 10000 | Maximum list IDs retained for child discovery, from 1 to 50000. |
| `max_requests` | No | 100000 | Maximum successfully processed collection responses during a scan, from 1 to 1000000. |
| `max_attempts` | No | 4 | Total HTTPS attempts per collection, from 1 to 10. |
| `request_timeout_ms` | No | 30000 | Total acquisition budget per collection including retries and parsing, from 100 to 300000 milliseconds. |
| `max_response_bytes` | No | 8388608 | Decoded response byte limit per collection, from 1024 to 67108864. |
| `endpoint` | No | `https://<sub_domain>.campayn.com` | Optional bare HTTPS origin paired with `trust_certificate`. |
| `trust_certificate` | No | | Worker-local X.509 PEM path for an explicit custom origin. |

## Output

The `campayn.records` table has five `STRING` columns.

| Column | Description |
| --- | --- |
| `resource` | Resource name. |
| `record_id` | Provider ID as text. |
| `sub_domain` | Configured tenant label normalized to lowercase. |
| `list_id` | The list record's own ID or the discovered list ID for a form or contact. Null for emails and reports. |
| `payload` | Complete provider object as JSON text, including optional fields, nested values and exact numbers. |

IDs for forms and contacts are scoped to their list. One ID can appear under different lists. If a form echoes a conflicting list ID, the source fails the collection before publishing rows.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Campayn {
    token = ${CAMPAYN_API_TOKEN}
    sub_domain = "your-tenant"
    resources = ["lists", "forms", "contacts", "emails", "reports"]
  }
}

sink {
  Console {}
}
```

## Collection bounds and recovery

The documented endpoints return complete root arrays and declare no continuation parameters. The source validates each whole array before publishing rows. Each collection permits up to 50,000 records and the configured decoded byte limit. An unexpected HTTP continuation header fails the collection.

Checkpoint state stores the selected collection, its parent traversal and validated partial-array progress. A partly emitted collection is fetched again and checked for identical content and order before its remaining rows are emitted. Source scope changes require a new scan. The API does not supply an isolated snapshot or delete feed for these reads.

The HTTPS client verifies the certificate and hostname, forbids redirects and bounds retries and cancellation. GET retries respect a valid `Retry-After` delay. Negotiated gzip counts against the decoded response limit.
