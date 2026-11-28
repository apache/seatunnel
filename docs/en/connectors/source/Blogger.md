<!--
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
-->

import ChangeLog from '../changelog/connector-http-blogger.md';

# Blogger

Install `connector-http-blogger` and use `Blogger` as a batch source with parallelism 1. Each source instance selects one resource and reads it through the [Blogger v3 API](https://developers.google.com/blogger/docs/3.0/reference). Enable Blogger for the OAuth client and configure a refresh token obtained with offline access and the `https://www.googleapis.com/auth/blogger.readonly` scope.

| Resource | Records |
| --- | --- |
| `users` | Blogs available to the authenticated user, requested through the self blog list. |
| `blogs` | Post listings for each of those blogs. |
| `posts` | Individual post details, using the blog and post IDs discovered together. |
| `pages` | Pages for each blog. |
| `comments` | Comments for each post in its owning blog. |

The connector discovers ancestors automatically. Only the selected resource emits rows. The Google account's permissions and the Blogger API's default visibility apply. Each row contains `blog_id`, `post_id` and `data`, all `STRING` fields. `post_id` is null for account blog and page rows. `data` is a JSON object that preserves the API's field names, numeric precision, nested values, nulls and missing optional fields. HTML, images and URL fields remain part of the JSON. The connector only sends requests to Blogger and the Google OAuth token service.

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `client_id` | string | — | Required Google OAuth client ID. |
| `client_secret` | string | — | Required Google OAuth client secret. |
| `token` | string | — | Required Google OAuth refresh token with offline Blogger read access. |
| `resource` | string | — | Required resource name from the table above. |
| `page_size` | int | `100` | Requested items per paginated list, 1–100. |
| `max_pages` | int | `2000` | Maximum pages in any list, 1–10000. |
| `max_blogs` | int | `5000` | Maximum records in the non-paginated account blog list, 1–50000. |
| `connect_timeout_ms` | int | `10000` | TCP connect timeout, 1–120000 milliseconds. |
| `read_timeout_ms` | int | `30000` | Socket read timeout, 1–120000 milliseconds. |
| `request_deadline_ms` | int | `60000` | Total page request budget including OAuth renewal and retries, 1–300000 milliseconds. |
| `max_attempts` | int | `4` | Maximum attempts per OAuth or data request, 1–10. |
| `max_response_bytes` | int | `8388608` | Independent encoded and decoded response limits, 1024–33554432 bytes. |

The connector renews expired access tokens and can renew a rejected bearer once during a page read. It verifies certificates and hostnames, rejects redirects, and bounds response size and retry delays. It detects malformed, expired and repeated page tokens and fails the job when it cannot safely continue.

Each batch starts a full traversal. Zeta checkpoints retain the parent and data positions. On restore, the source validates the ordered identities of in-progress parents and the active page before using a saved row offset. If the replay changes or Google rejects a saved token, restore fails explicitly. The provider does not offer an account-wide consistent snapshot during a scan. Completed scans remain complete on restore. Start a new read when the OAuth principal, resource or page size changes.

## Example

```hocon
env {
    job.mode = "BATCH"
    parallelism = 1
}
source {
    Blogger {
        client_id = "example.apps.googleusercontent.com"
        client_secret = ${BLOGGER_CLIENT_SECRET}
        token = ${BLOGGER_REFRESH_TOKEN}
        resource = "comments"
    }
}
sink {
    Console {}
}
```

<ChangeLog />
