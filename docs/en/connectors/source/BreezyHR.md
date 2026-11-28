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

# Breezy HR Source

The `BreezyHR` source reads recruiting data from the Breezy HR v3 API. It performs a bounded scan of one company at source parallelism 1. Install `connector-http-breezyhr` if the connector is absent from your distribution.

## Resources

| Resource | API operation | Output |
| --- | --- | --- |
| `positions` | `GET /v3/company/{company_id}/positions` | One row per position. The provider excludes archived positions by default. |
| `candidates` | `GET /v3/company/{company_id}/position/{position_id}/candidates` | One row per candidate per discovered position. Position discovery also runs when `positions` output is disabled. |
| `pipelines` | `GET /v3/company/{company_id}/pipelines` | One row per entry in the response map. The entry key and its full pipeline object, including ordered stages, are retained. |

Use a personal access token created in **My Settings → API Keys** or an existing session token. Set `company_id` to the `_id` returned by `GET /v3/companies`. The source sends the token directly as the `Authorization` value. The token owner needs company access and permission to read candidates for each position in scope. A readable position can still deny candidate access. Such a denial fails the scan with the affected operation and HTTP status. [Authentication](https://developer.breezy.hr/reference/authorization) · [Positions](https://developer.breezy.hr/reference/listpositions) · [Candidates](https://developer.breezy.hr/reference/listcandidates) · [Pipelines](https://developer.breezy.hr/reference/listpipelines)

## Options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `token` | Yes | | Bare personal access or session token. |
| `company_id` | Yes | | Company API identifier. Use up to 128 letters, digits, underscores or hyphens. |
| `resources` | Yes | | Non-empty list of unique resource names from the table above. |
| `page_size` | No | 50 | Number of records requested per positions or candidates page, from 1 to 50. |
| `max_pages_per_scope` | No | 1000 | Successful pages permitted for the positions list and separately for each position's candidates. Includes the last empty page. Range 1–10000. |
| `max_requests` | No | 100000 | Successful fully processed responses permitted across the scan. Range 1–1000000. |
| `max_attempts` | No | 4 | Total attempts per page including the first. Range 1–10. |
| `request_timeout_ms` | No | 30000 | Total acquisition deadline for each page including retries and parsing. Range 100–300000 ms. |
| `max_response_bytes` | No | 8388608 | Decoded response limit in bytes. Range 1024–67108864. |
| `endpoint` | No | `https://api.breezy.hr` | Bare HTTPS origin. An explicitly configured origin requires `trust_certificate`. |
| `trust_certificate` | No | | Worker-local X.509 PEM file for the custom HTTPS origin. |

## Output

The `breezyhr.records` table has the following columns, all of type `STRING`.

| Column | Description |
| --- | --- |
| `resource` | Selected resource name. |
| `record_id` | Textual provider `_id` for a position or candidate, or the pipeline map key. A candidate ID can occur under more than one position. |
| `position_id` | The position's own ID or the candidate's parent position ID. Null for pipelines. |
| `position_type` | Position `type.name` when present. |
| `country_id` | Position `location.country.id` when present. |
| `country_name` | Position `location.country.name` when present. |
| `stage_name` | Candidate `stage.name` when present. |
| `payload` | Full provider object as JSON text with nested values, extra fields and numeric precision retained. |

Optional projections are null when absent or inapplicable. For candidate storage keys within one company, use both `position_id` and `record_id`. Candidate summaries can include contact details and resume URLs. The source reads the summaries and leaves URLs as data.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  BreezyHR {
    company_id = "6a1ef2bc42b0"
    token = ${BREEZY_HR_TOKEN}
    resources = ["positions", "candidates", "pipelines"]
  }
}

sink {
  Console {}
}
```

## Paging and recovery

Positions and candidates use 1-based pages sorted by `updated`. The source follows each list until an empty page, including after a short page. Each discovered position gets its own candidate list and page counter. The pipeline map is read once and limited to 10,000 entries and the configured response size.

A checkpoint tracks the current request, pending position IDs, recent page identity evidence and the number of published rows in the current page. Partial-page restore re-reads that page and checks its identity and payload before emitting the remainder. Changing the token, company, resource selection, origin, certificate path or page size requires a fresh scan. The source rejects duplicates within a page, overlap between adjacent pages and repeated prior pages. Breezy HR's page-number API offers no snapshot isolation. Changes to the underlying lists can still affect membership or order during a scan. The source provides bounded reads with checkpointing.

The HTTPS client validates the hostname and certificate and forbids redirects. It retries eligible GET failures within the configured attempts and deadline. For HTTP 429 it requires a usable `Retry-After` or `X-RateLimit-Reset` and waits for the largest indicated delay. The Breezy HR guide describes the reset as a timestamp without specifying a unit. The source accepts 10-digit epoch seconds, 13-digit epoch milliseconds and explicit timestamp dates. Malformed timing or a delay beyond the acquisition deadline fails the request. [Breezy HR rate limits](https://developer.breezy.hr/reference/rate-limiting)
