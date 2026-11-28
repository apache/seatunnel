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

import ChangeLog from '../changelog/connector-http-apify-dataset.md';

# ApifyDataset

ApifyDataset reads dataset metadata and items from Apify Storage. Use batch mode with source parallelism 1. The connector uses a bearer token over HTTPS and reads each selected resource in order.

## Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)

## Resources

| Resource | Endpoint | Row payload |
| --- | --- | --- |
| `dataset_collection` | `GET /v2/datasets` | One dataset metadata object per row, in creation order. Apify lists named datasets by default. |
| `dataset` | `GET /v2/datasets/{datasetId}` | One metadata object for the selected dataset. |
| `item_collection_website_content_crawler` | `GET /v2/datasets/{datasetId}/items` | One original item object per row, including nested crawler and custom fields. |
| `item_collection` | `GET /v2/datasets/{datasetId}/items` | One `{"data": {...}}` object per row, with the complete item inside `data`. |

Each item resource reads the same dataset export in its requested layout. Raw JSON preserves hidden fields and empty items. Datasets can be selected by their ID or by a name such as `username~dataset-name`. [Apify API](https://docs.apify.com/api/v2)

## Source options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | String | Yes | — | Apify API bearer token. |
| `dataset_id` | String | Conditional | — | Provider dataset ID or named reference. Required for metadata and both item resources. |
| `resources` | List of strings | No | All four in the order above | Ordered, unique resource names. |
| `api_url` | String | No | `https://api.apify.com` | HTTPS origin for the provider hostname, with optional port. |
| `page_size` | Integer | No | `50` | Maximum raw scan per page, from 1 to 1000. |
| `max_pages` | Integer | No | `10000` | Maximum pages per resource, from 1 to 100000. An unfinished scan at this limit fails the job. |
| `max_response_bytes` | Integer | No | `16777216` | Separate encoded and decoded response byte limits, from 1024 to 33554432. |
| `connect_timeout_ms` | Integer | No | `10000` | TCP connection timeout, from 1 to 300000. |
| `read_timeout_ms` | Integer | No | `10000` | TLS and response inactivity timeout, from 1 to 300000. |
| `request_timeout_ms` | Integer | No | `30000` | Total timeout per page including retries and delays, from 1 to 300000. |
| `max_attempts` | Integer | No | `4` | Total attempts per page including the initial request, from 1 to 10. |

## Output and restore

The table has three string columns.

| Column | Meaning |
| --- | --- |
| `resource` | Resource name from the table above. |
| `dataset_id` | Provider ID for metadata rows and the configured dataset reference for item rows. |
| `payload` | JSON text with the resource's row object. Field names, nested arrays, nulls, and precise JSON numbers are retained. |

Dataset metadata omits top-level `urlSigningSecretKey` and `itemsPublicUrl` because those fields can grant dataset access. Item payloads preserve business fields with those names.

Each paginated resource uses its first page's total as the upper scan bound. Item pages advance by the provider's scanned row count, allowing short and empty result arrays to consume their covered positions. Appends beyond the first total are excluded. If the provider reports a smaller total, repeats an offset, stalls, or returns more item records than the reported scan, the job fails. Apify's item counters can briefly lag recent writes. Use a completed dataset for a reproducible export. [Dataset items API](https://docs.apify.com/api/v2/dataset-items-get) · [Dataset metadata API](https://docs.apify.com/api/v2/dataset-get)

A checkpoint records the resource, bounded range and page offset. For a partially emitted page, restore fetches the same logical page, verifies the response digest and resumes at the next saved row. Restores resume at the pending page or resource after a completed boundary. Pages consumed earlier can change independently on Apify.

TLS uses the JVM trust store and checks the provider hostname. Redirects and permanent authorization or missing dataset errors fail the job. Temporary 429, 500, 502, 503 and 504 status codes can retry within the bounded per-page deadline. The configured API token is omitted from source errors and checkpoint state.

## Example

Pass the API token as a SeaTunnel job variable.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  ApifyDataset {
    token = "${APIFY_API_TOKEN}"
    dataset_id = "my-account~product-catalog"
    resources = ["dataset", "item_collection"]
    page_size = 50
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
