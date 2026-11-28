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

import ChangeLog from '../changelog/connector-http-wordpress.md';

# WordPress

Reads published posts from one public WordPress.com site using the [WordPress.com Posts API](https://developer.wordpress.com/docs/api/1/get/sites/$site/posts/). It performs unauthenticated HTTPS reads against `public-api.wordpress.com`; it does not write to the site or connect directly to a self-hosted WordPress installation.

## Supported behavior

- Bounded batch reads only; set job parallelism to 1.
- This connector does not support private sites, drafts, pages, comments, custom post types, or streaming/CDC.
- The window filters the **publication** timestamp, not the timestamp of later edits. It does not report deletes.
- Reads are ordered by ascending post ID. Changing totals, inconsistent page sizes, out-of-order or repeated IDs, malformed responses, HTTP failures (including 429), and results above the page limit fail the job. Stable totals and order cannot detect every concurrent edit; this is not a transactional snapshot. Prefer an older, stable window.
- Restarting a job replays the window. There is no remote checkpoint or exactly-once guarantee; a nontransactional sink may have committed rows before a failure. For replay-safe loads, use the site and post ID as a destination key.
- Zeta BATCH jobs enable periodic engine checkpoints only when `checkpoint.interval` is set in `env`. Those checkpoints do not persist remote page progress; restored readers replay the configured window.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `site` | string | Yes | None | A numeric WordPress.com site ID or public site hostname, without a scheme, path, port, or query. Prefer the site ID if a domain can change. |
| `start_date` | string | Yes | None | Inclusive lower publication-time limit as UTC ISO 8601 with second precision (for example, `2005-09-01T00:00:00Z`). |
| `end_date` | string | Yes | None | Inclusive upper publication-time limit, same format and at or after `start_date`. |
| `page_size` | int | No | 50 | Posts to request per page, from 1 to 100. |
| `max_pages` | int | No | 1000 | Maximum number of pages, from 1 to 10000. The connector fails instead of truncating a larger result. |

Both bounds are inclusive. Use nonoverlapping second-precision windows or deduplicate at the sink when composing exports.

## Output schema

| Field | Type | Description |
| --- | --- | --- |
| `id` | BIGINT | WordPress post ID, unique within the site. |
| `slug` | STRING | Post slug. |
| `title` | STRING | API display title, which may contain HTML entities. |
| `url` | STRING | Public post URL. |
| `published_at` | TIMESTAMP | Publication timestamp normalized to UTC. |
| `modified_at` | TIMESTAMP | Last modification timestamp normalized to UTC. |

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  WordPress {
    site = "en.blog.wordpress.com"
    start_date = "2005-09-01T00:00:00Z"
    end_date = "2005-09-30T23:59:59Z"
    page_size = 1
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
