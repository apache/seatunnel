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

import ChangeLog from '../changelog/connector-http-activecampaign.md';

# ActiveCampaign

The ActiveCampaign source reads campaign, contact, list, deal, segment, and form collections from an account using the ActiveCampaign API v3. Install `connector-http-activecampaign` and use `ActiveCampaign` in a batch job.

Use the API URL from **Settings → Developer** as `base_url`, keeping only its HTTPS origin. The API token uses the `Api-Token` request header. The connector validates TLS certificates and the configured hostname and rejects HTTP redirects. [ActiveCampaign base URL](https://developers.activecampaign.com/reference/url)

## Output

| Column | Type | Description |
| --- | --- | --- |
| `resource` | STRING | `campaigns`, `contacts`, `lists`, `deals`, `segments`, or `forms` |
| `id` | STRING | The record's identifier, also retained in `record` with its original JSON type |
| `record` | STRING | Serialized JSON object, including nested fields, nulls and full numeric precision |

The source emits records from each resource's array. ActiveCampaign permissions can restrict fields in deal records. The lists endpoint's default returns email lists. The segment resource reads the v3 `/segments` collection.

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `base_url` | String | Required | HTTPS origin from the account's Developer settings, with an optional port |
| `token` | String | Required | ActiveCampaign API token |
| `resources` | List of strings | All six | Unique resource names in scan order |
| `page_size` | Integer | `20` | Requested limit, 1–100 |
| `max_pages` | Integer | `10000` | Maximum pages per resource, 1–100000 |
| `request_interval_ms` | Integer | `250` | Minimum delay between request starts for this job, 200–60000 ms |
| `max_attempts` | Integer | `4` | Maximum attempts per page, including the first, 1–8 |
| `socket_timeout_ms` | Integer | `15000` | Per-attempt connect and read inactivity timeout, 100–120000 ms |
| `page_deadline_ms` | Integer | `60000` | Total time for requests, retries, and preparation of a page, 500–600000 ms |
| `max_response_bytes` | Integer | `8388608` | Decompressed response size ceiling per page, 128–33554432 bytes |

## Pagination and recovery

One source reader scans the selected resources sequentially. ActiveCampaign limits each account to five requests per second, shared with other jobs and applications. The connector spaces its own requests and uses bounded retries, respecting valid `Retry-After` values within its page deadline. [ActiveCampaign rate limits](https://developers.activecampaign.com/reference/rate-limits)

The source advances the offset by the requested page size. With a valid `meta.total`, it can continue after short or empty pages. Without a total, a short page ends the resource. Repeated pages, excessive empty continuation, and invalid page metadata fail the scan with a bounded error. [ActiveCampaign pagination](https://developers.activecampaign.com/reference/pagination)

A checkpoint saves the resource, page offset, and emitted row position. For a partial page, the source fetches that page again and verifies its content before skipping emitted rows. Resume with the same origin, resources in the same order, and page size. Remote changes can alter the page contents or offsets, and the API supplies no cross-page snapshot. Use a downstream key such as `(resource, id)` when consuming collections that change during a scan.

## Example

```hocon
env {
  job.mode = "BATCH"
}

source {
  ActiveCampaign {
    base_url = "https://account.api-us1.com"
    token = "${activecampaign_token}"
    resources = ["campaigns", "contacts", "lists", "deals", "segments", "forms"]
    page_size = 20
  }
}

sink {
  Console {}
}
```

<ChangeLog />
