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

import ChangeLog from '../changelog/connector-http-akeneo.md';

# Akeneo

Install `connector-http-akeneo` and configure `Akeneo` in a batch job. Create an Akeneo PIM Connection with the required read permissions and supply its client ID, client secret, API username, and password. The connector uses the private Connection password grant and refreshes access tokens. It verifies the HTTPS certificate and hostname and rejects redirects. [Akeneo authentication](https://api.akeneo.com/documentation/authentication.html)

## Resources

| Resource | GET path under `/api/rest/v1` |
| --- | --- |
| `products` | `/products-uuid` |
| `categories` | `/categories` |
| `families` | `/families` |
| `family_variants` | `/families/{code}/variants` |
| `attributes` | `/attributes` |
| `attribute_groups` | `/attribute-groups` |
| `association_types` | `/association-types` |
| `channels` | `/channels` |
| `locales` | `/locales` |
| `currencies` | `/currencies` |
| `measure_families` | `/measurement-families`, `/measure-families` |

Omit `resources` to read all 11 collections. Selecting only `family_variants` still discovers families and reads each family’s variants, while emitting only variant rows. The product UUID endpoint is documented for Akeneo SaaS and PIM 7 Community and Enterprise editions. The current measurement endpoint returns an array. Set `measurement_api = "legacy"` if your deployment exposes the older paginated endpoint. Resource availability and visibility depend on the deployment and permissions. [REST reference](https://api.akeneo.com/api-reference.html) · [PIM 7 endpoint index](https://api.akeneo.com/api-reference-paas-index-70.html)

Rows contain `resource STRING`, `id STRING`, nullable `parent_id STRING` and `record STRING`. The ID is a UUID for products and a code for other resources. A family variant’s parent ID is its family code. The JSON record preserves the provider object, including localized and scoped values, nested data, nulls, and numeric precision.

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `base_url` | String | Required | PIM HTTPS origin |
| `client_id` | String | Required | Connection client ID |
| `client_secret` | String | Required | Connection client secret |
| `username` | String | Required | Connection API username |
| `password` | String | Required | Connection API password |
| `resources` | List of strings | All | Unique resource names from the table |
| `measurement_api` | String | `current` | `current` for the array endpoint or `legacy` for paginated measure families |
| `page_size` | Integer | `100` | Page size, 1–100 |
| `max_pages` | Integer | `100000` | Maximum pages per collection or family, 1–100000 |
| `request_interval_ms` | Integer | `25` | Minimum time between requests, 10–60000 ms |
| `max_attempts` | Integer | `4` | Attempt limit per eligible operation, 1–8 |
| `socket_timeout_ms` | Integer | `15000` | Connect/read inactivity timeout, 100–120000 ms |
| `page_deadline_ms` | Integer | `60000` | Elapsed time per page including authorization and retries, 500–600000 ms |
| `max_response_bytes` | Integer | `8388608` | Decompressed response size, 1024–33554432 bytes. Token responses also have a 64 KiB limit. |

## Pagination and recovery

Products follow Akeneo’s opaque search-after cursor. Other paginated collections use page numbers. The connector checks continuation scope, rejects cursor cycles, and fails if reaching the configured page limit would omit records. A single reader paces requests. Read retries honor valid Retry-After hints within the page deadline. A token POST is retried only after an explicit HTTP 429 because an interrupted refresh may have invalidated the old token. [Akeneo pagination](https://api.akeneo.com/documentation/pagination.html) · [Akeneo rate limits](https://api.akeneo.com/documentation/good-practices.html)

Checkpoints retain resource, parent and child page positions and hashes of partially processed pages. Recovery reauthenticates and verifies the saved page before skipping processed rows. Retain the origin, client ID, API username, resource list, measurement API and page size on restore. Password and client secret may rotate. Catalog changes can affect later pages. Every new job starts a full scan with no incremental watermark.

## Example

```hocon
env {
  job.mode = "BATCH"
}

source {
  Akeneo {
    base_url = "https://your-pim.example.com"
    client_id = "${akeneo_client_id}"
    client_secret = "${akeneo_client_secret}"
    username = "${akeneo_api_username}"
    password = "${akeneo_api_password}"
    resources = ["products", "family_variants", "measure_families"]
  }
}

sink {
  Console {}
}
```

<ChangeLog />
