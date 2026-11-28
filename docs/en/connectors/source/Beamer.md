import ChangeLog from '../changelog/connector-http-beamer.md';

{/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */}

# Beamer

Install `connector-http-beamer` and set the source name to `Beamer`. The Beamer source reads Net Promoter Score (NPS) responses. It makes HTTPS GET requests to `/v0/nps` using the `Beamer-Api-Key` header. Create an API key in Beamer under **Settings > API** and grant it access to NPS responses. [Beamer API documentation](https://getbeamer-api.pages.dev/)

This is a bounded scan with source parallelism 1. It returns one table named `nps`.

| Column | Type | Description |
| --- | --- | --- |
| `record_id` | STRING | The provider's response ID, or null when omitted or null. |
| `created_at` | STRING | The response's creation time in UTC ISO 8601. Fractional seconds are preserved. |
| `payload` | STRING | The full response object as JSON, including feedback, score, optional fields and additional fields from Beamer. |

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | STRING | Yes | | The Beamer API key. |
| `start_date` | STRING | Yes | | Inclusive creation time in RFC 3339 with an offset. |
| `end_date` | STRING | No | Start of the scan | Exclusive creation time in RFC 3339 with an offset. |
| `page_size` | INT | No | 100 | Beamer `maxResults`, from 1 to 100. |
| `max_pages` | INT | No | 1000 | Successful page requests allowed, including the last empty page. Range 1 to 5000. |
| `max_attempts` | INT | No | 4 | Total attempts per page for retryable failures. Range 1 to 10. |
| `request_timeout_ms` | INT | No | 30000 | Total page acquisition deadline, including retries. Range 100 to 300000. |
| `max_response_bytes` | INT | No | 8388608 | Maximum decoded size of a successful response in bytes. Range 1024 to 67108864. |
| `endpoint` | STRING | No | `https://api.getbeamer.com` | Custom HTTPS origin with no path, query, fragment or credentials. Requires `trust_certificate`. |
| `trust_certificate` | STRING | No | | Worker-local PEM trust anchor for a custom origin. TLS hostname verification remains enabled. |

## Windows and recovery

The source includes responses created at `start_date` and excludes those at `end_date`. It selects an omitted end once when the scan begins and saves that time in checkpoints. To cover provider time boundaries, it requests Beamer from one second before the lower bound rounded down and rounds the upper bound up to a whole second. It applies the exact interval before emitting records.

Requests start at `page=0` with the same date filters and `maxResults`. A short page or a page containing only boundary records still advances to the next page. An empty array finishes the scan. The source fails on repeated pages, duplicate IDs in the same or adjacent pages, invalid responses or a page limit reached before the last empty page. A restored partial page must match the saved response fingerprint before its remaining rows can be emitted.

The scan uses the response creation date. Beamer's page-number API does not provide an isolated snapshot if responses change during a scan. Keep the account and date window stable during recovery. New runs can use consecutive half-open windows, with the end of one used as the start of the next.

Beamer documents HTTP 429 for this endpoint as account request quota exhaustion. The source reports it immediately. Transient request or server failures use the configured attempt and time limits and honor valid `Retry-After` delays on retryable responses. The source follows no redirects and omits credentials, response bodies and raw request URLs from diagnostics. Feedback payloads can include personal information such as email addresses.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Beamer {
    token = ${BEAMER_API_KEY}
    start_date = "2024-03-01T00:00:00Z"
    end_date = "2024-04-01T00:00:00Z"
  }
}
```

## Changelog

<ChangeLog />
