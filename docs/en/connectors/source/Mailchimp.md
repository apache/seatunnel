import ChangeLog from '../changelog/connector-http-mailchimp.md';

# Http-Mailchimp

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

> Mailchimp list members source connector

## Description

Reads the members of one configured audience using the Mailchimp Marketing API
[`GET /3.0/lists/{list_id}/members`](https://mailchimp.com/developer/marketing/api/lists/members/list-members/).
Only bounded batch reads are supported. Campaigns, reports, OAuth and CDC are not supported.
Uses the existing HTTP and JSON dependencies; no Mailchimp SDK is required.

## Key Features

- [x] Batch
- [ ] Stream
- [ ] Exactly-once
- [ ] Column projection
- [ ] Parallelism

Set source parallelism to 1.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| token | String | Yes | - | Mailchimp API key including its datacenter suffix, for example `<key>-us1`. Not an OAuth token. |
| list_id | String | Yes | - | One audience/list ID. Letters, digits, underscores and hyphens only. |
| schema | Config | Yes | - | Nonempty output schema using SeaTunnel's existing JSON format. |
| base_url | String | No | `https://<dc>.api.mailchimp.com/3.0` | Explicitly trusted API root. The default datacenter comes from `token`. |
| allow_insecure_http | Boolean | No | `false` | Allow HTTP only for isolated test fixtures with dummy credentials. Never enable with real API keys. |
| page_size | Int | No | `1000` | Members per page, from 1 to 1000. |
| retry | Int | No | `3` | Additional attempts per page on HTTP 429 or 5xx, from 0 to 10. |
| retry_backoff_ms | Int | No | `1000` | Initial retry delay, from 1 to 60000 milliseconds. Doubles up to 60000 milliseconds. |
| request_timeout_ms | Int | No | `30000` | Connection, pool acquisition and socket inactivity timeout, from 1 to 300000 milliseconds. Not a total batch deadline. |

### Authentication and Endpoint Trust

The `token` option holds an API key and uses SeaTunnel's existing sensitive-option masking.
It is sent only in the HTTP Basic Authorization header (username `seatunnel`, password the API key).
The connector does not log request headers, response bodies or member data, and its split state
contains no credentials or member data. Standard engine configuration and secret-management rules
still apply. Do not enable HTTP wire/header logging with real credentials.

A custom `base_url` is an administrator-selected credential destination: use only an API endpoint
you trust. It must use HTTPS and end in `/3.0` (an optional trailing slash is accepted), with a host
and optional port, but no user information, query, fragment or other path. HTTP requires explicit
`allow_insecure_http = true` and is for isolated dummy-credential fixtures only.
Redirects, cookies and automatic HTTP-client retries are disabled; a redirect fails without forwarding
credentials. Response-provided links are never followed.

### Schema

Each member object is decoded independently using the existing JSON format. Typical fields are
`id`, `email_address`, `status`, `last_changed` and `merge_fields`.
Field names match the API JSON keys. `last_changed` can be read as a STRING to retain its ISO-8601
offset. Missing optional fields become null; invalid type conversions fail without including the
member data in the error.

`merge_fields` is not flattened. Use a nested ROW for known heterogeneous fields or a MAP when
values have a consistent type. The example uses `map<string,string>`; configure a matching schema
for your actual merge-field definitions. A field declared STRING uses the existing JSON format's
conversion rules, not a connector-specific JSON serializer.

## Example

```hocon
env {
  parallelism = 1
  job.mode = BATCH
}
source {
  Mailchimp {
    token = "<MAILCHIMP_API_KEY-us1>"
    list_id = "<LIST_ID>"
    page_size = 1000
    schema {
      fields {
        id = string
        email_address = string
        status = string
        last_changed = string
        merge_fields = "map<string,string>"
      }
    }
  }
}
sink {
  Console {}
}
```

## Pagination, Recovery and Consistency

Requests send `count` (at most 1000) and a zero-based `offset`. The first successful page's
`total_items` sets the batch's finite upper bound. Later growth cannot extend that bound.
The final request reduces `count` to the remaining number of members.

Each page must contain a `members` array and a nonnegative integer `total_items` within the API's
integer offset range. A page must return the expected number of member objects. A short/empty
page before completion, shrinking total, invalid JSON, unexpected shape or schema conversion
failure fails the batch instead of silently reporting success. Responses are limited to 32 MiB
per page; lower `page_size` for large merge fields.

This is **not a transactional or exact snapshot**. Concurrent additions, deletions and updates can
shift offsets, duplicate members or omit members even when counts match. For a stable export, avoid
audience changes during the run. No `since_last_changed` filter or durable incremental cursor is
provided.

Recovery deliberately replays the **whole batch from offset zero**, including previously accepted
pages. This is at-least-once batch replay, not exactly-once delivery or a guarantee of capturing
every version of a changing member. Use member `id` for downstream deduplication/upserts.
Checkpoints do not persist pagination offsets or a completed-batch marker. Network waits do not
hold the collector's checkpoint lock.

HTTP 429 and 5xx responses are retried at the same offset up to `retry` times. `Retry-After`
seconds or HTTP dates are honored with a maximum wait of 60 seconds; invalid values use exponential
backoff. Other non-200 responses (including 401 and redirects), malformed bodies and transport
failures fail immediately. Restarting the job replays the batch. Cancellation during reading aborts active I/O
and wakes backoff waits without signalling successful completion.

<ChangeLog />
