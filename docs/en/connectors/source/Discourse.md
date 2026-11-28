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

import ChangeLog from '../changelog/connector-http-discourse.md';

# Discourse

The Discourse source reads category or recent-topic metadata from a Discourse site using the
[Discourse API](https://docs.discourse.org/). It makes read-only requests, supports BATCH mode only,
and requires source parallelism 1. Each source instance reads one resource.

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `base_url` | String | Yes | | Site root, optionally including its deployment subpath. Do not include an endpoint, query string, fragment, or credentials. |
| `resource` | Enum | Yes | | `CATEGORIES` or `LATEST_TOPICS`. |
| `include_subcategories` | Boolean | No | `true` | For `CATEGORIES` only. Ask the API to include subcategories and flatten returned nested records into rows. |
| `topic_limit` | Integer | No | `30` | For `LATEST_TOPICS` only. Requested size of a single page, from 1 through 100; server policy may return fewer. |
| `token` | String | No | | Set together with `username` to authenticate over HTTPS. Omit both for public data if your site permits anonymous access. |
| `username` | String | No | | Discourse API username. Must be set with `token`. |

The connector sends credentials in `Api-Key` and `Api-Username` headers, not in the URL, and
does not follow redirects. Use a least-privilege key and SeaTunnel's secret or variable
configuration instead of writing real credentials in an example or checked-in job. The connector
does not retry failed requests, including HTTP 429. If rate limited, follow the site's retry policy
before starting another job.

## Output

`CATEGORIES` calls `GET /categories.json`. It emits `id` (BIGINT), `name` and `slug` (STRING),
and `parent_category_id`, `topic_count`, and `post_count` (nullable BIGINT). Top-level categories
have no parent. IDs are scoped to the site and resource, not globally unique across sites.
Only the categories and descendants returned in this single response are read. Site lazy-loading
or pagination policy may omit other categories; `include_subcategories` does not guarantee all descendants.

`LATEST_TOPICS` calls `GET /latest.json`. It emits `id` (BIGINT), `title` and `slug` (STRING),
`category_id`, `posts_count`, and `views` (nullable BIGINT), `created_at` and `last_posted_at`
(nullable STRING, timestamp text as returned by Discourse), and `closed` and `archived`
(nullable BOOLEAN).

The output reflects only objects visible to the configured identity. A run does not follow more-topic
links and is not a full forum export, a consistent history snapshot, or an incremental sync. It does
not read individual posts, message bodies, user records, or audit data. Reruns may return overlapping
or different rows. Use the source's `id` and the site identity if you need to deduplicate in a sink.
Decoded responses larger than 16 MiB fail the job rather than producing partial results. The HTTP
fetch has a 30-second elapsed deadline, including continuously progressing responses. A timeout
fails the fetch without emitting partially decoded rows. Responses are limited to 100 headers and
16 KiB per status/header line.

The complete response is validated before any row is emitted. The reader retains that response
and emits at most one row per poll, allowing the engine's cooperative checkpoint handoff between
rows without another HTTP request. A single collection can still wait on downstream progress;
this is not a wall-clock checkpoint guarantee. Checkpoints keep the existing null split state,
not the prepared response or row position. A restored reader fetches the whole current response
again, so already emitted rows can repeat or change; this does not provide exactly-once delivery.

## Example

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}
source {
  Discourse {
    base_url = "https://meta.discourse.org"
    resource = LATEST_TOPICS
    topic_limit = 5
    plugin_output = "recent_discourse_topics"
  }
}
sink {
  Console {
    plugin_input = "recent_discourse_topics"
  }
}
```

## Changelog

<ChangeLog />
