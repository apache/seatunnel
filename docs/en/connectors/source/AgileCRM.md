import ChangeLog from '../changelog/connector-http-agilecrm.md';

# AgileCRM

{/*
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
*/}

The `AgileCRM` source reads one bounded resource from the Agile CRM REST API. Install `connector-http-agilecrm` on the engine. Use your Agile CRM account email and API key, with access to the selected resource and any of its parent lists. [Agile CRM REST API](https://github.com/agilecrm/rest-api)

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `base_url` | String | Yes | | Trusted tenant origin, for example `https://example.agilecrm.com`, with no path, query, fragment, or credentials. |
| `username` | String | Yes | | Agile CRM account email used for Basic authentication. |
| `password` | String | Yes | | Agile CRM API key used for Basic authentication. |
| `resource` | String | Yes | | One resource from the table below. |
| `page_size` | Integer | No | 100 | Requested page size for cursor lists, from 1 to 100. The request size for `tickets` is capped at 25. |
| `event_start` | Long | For `events` | | Nonnegative start of the requested event window, in Unix seconds. |
| `event_end` | Long | For `events` | | End of the requested event window, in Unix seconds. It must be greater than `event_start`. |
| `allow_insecure_http` | Boolean | No | false | Allows cleartext HTTP for an explicitly trusted, isolated origin. |
| `request_timeout_ms` | Integer | No | 30000 | Total request deadline, counting retry delays, from 1 to 300000 milliseconds. |
| `max_attempts` | Integer | No | 4 | Maximum HTTP attempts per request, from 1 to 8. |
| common-options | | No | | [Source common options](../common-options/source-common-options.md). |

## Resources

| Resource | Read operation |
| --- | --- |
| `contacts` | Cursor list. |
| `companies` | Read-only list POST with form-encoded page size, sort key, and cursor. |
| `deals` | Cursor list. |
| `notes` | Notes for each discovered contact. |
| `tasks` | Task list. |
| `events` | Events in the configured time window. |
| `milestone` | Deal pipelines and their milestones. |
| `campaigns` | Campaign list with cursor pagination. A single campaign object is also accepted. |
| `documents` | Document metadata for each discovered contact. Attachment URLs are included as metadata. |
| `ticket_filters` | Visible ticket filters. |
| `tickets` | Tickets for each discovered filter, using the documented filtered list request. |

The cursor lists read the opaque token from the last record. A short page can continue. An empty array or an absent, null, or empty last-record cursor ends the list. Discovery uses unfiltered parent lists and deduplicates parent IDs. Child records retain their parent IDs, so a ticket shared by distinct filters can appear once for each filter.

The ticket list documentation does not specify how to request a continuation. The source reads short results for each filter and fails when a response includes a continuation marker or reaches the requested size. This prevents accepting a potentially truncated filter. Document links are returned in the payload without downloading the files.

Each row has `resource STRING`, nullable `record_id STRING`, nullable `parent_id STRING`, and `payload STRING`. The payload contains the JSON object and preserves nested values and numeric precision. Parent IDs identify contacts for notes/documents and filters for tickets. Payloads can contain customer information.

## Reads and recovery

Use source parallelism 1. The scan is bounded and starts from the list beginning in a new job. The checkpoint captures traversal position, IDs and cursors without credentials or record payloads. Restore requires the same origin, resource, requested page size and event range. A rotated API key can be supplied upon restore. If the provider expires the cursor or data changes during the scan, recovery can fail or re-emit records after the most recent checkpoint.

The source fails when a response exceeds 8 MiB before or after decompression or exceeds 10000 records. Other limits are 10000 pages per list, 200000 accepted pages per scan and 100000 discovered parent IDs. Repeated cursors fail before emitting their page.

Requests verify HTTPS certificates and hostnames by default. Redirects and authentication or authorization errors fail immediately. Only returned HTTP 429, 500, 502, 503, and 504 are eligible for bounded retry, including on the read-only company listing. A provider-supplied `Retry-After` lower bound must fit the request budget.

## Example

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}

source {
  AgileCRM {
    base_url = "https://example.agilecrm.com"
    username = ${?AGILE_CRM_EMAIL}
    password = ${?AGILE_CRM_API_KEY}
    resource = "documents"
    page_size = 50
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
