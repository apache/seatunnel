<!-- Licensed to the Apache Software Foundation (ASF) under one or more
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

import ChangeLog from '../changelog/connector-http-adobe-commerce.md';

# AdobeCommerce

`AdobeCommerce` reads traditional Adobe Commerce and Magento Open Source REST v1 data with a bounded batch job. Install `connector-http-adobe-commerce` and configure the URL of the deployment over HTTPS. Use a bearer token that the deployment already accepts and that has permission to read the selected resources. Adobe Commerce as a Cloud Service uses the separate IMS API.

The connector appends `/rest/V1` to the deployment URL or `/rest/<store_code>/V1` when a store code is configured. For a site installed in a subdirectory, include the subdirectory in `base_url`. The TLS certificate must match the URL host. Renew expiring admin tokens outside the connector and supply a valid token on restore. Adobe Commerce 2.4 deployments disable standalone integration bearer tokens by default. [REST API overview](https://developer.adobe.com/commerce/webapi/rest/) · [Token authentication](https://developer.adobe.com/commerce/webapi/get-started/authentication/gs-authentication-token)

## Resources

| Resource | GET route under v1 | Date field for a bounded window |
| --- | --- | --- |
| `carts` | `/carts/search` | `updated_at` |
| `orders` | `/orders` | `updated_at` |
| `coupons` | `/coupons/search` | `created_at` |
| `invoices` | `/invoices` | `updated_at` |
| `products` | `/products` | `updated_at` |
| `customers` | `/customers/search` | `updated_at` |
| `shipments` | `/shipments` | `updated_at` |
| `tax_rates` | `/taxRates/search` | Full scan |
| `creditmemos` | `/creditmemos` | `updated_at` |
| `sales_rules` | `/salesRules/search` | `from_date` |
| `store_views` | `/store/storeViews` | Full scan |
| `tax_classes` | `/taxClasses/search` | Full scan |
| `store_groups` | `/store/storeGroups` | Full scan |
| `transactions` | `/transactions` | `created_at` |
| `store_websites` | `/store/websites` | Full scan |
| `customer_groups` | `/customerGroups/search` | Full scan |
| `inventory_stocks` | `/inventory/stocks` | Full scan |
| `inventory_sources` | `/inventory/sources` | Full scan |
| `directory_currency` | `/directory/currency` | Full scan |
| `directory_countries` | `/directory/countries` | Full scan |
| `inventory_source_items` | `/inventory/source-items` | Full scan |

The three store endpoints and countries each return an unpaged array. Currency returns one object. The other resources use one-based `searchCriteria` pagination. One source reader visits the requested resources in order. Inventory resources require the deployment's Inventory Management API.

Each row contains `resource STRING`, `id STRING`, and `record STRING`. The last column contains the complete JSON object with nested values, Unicode, nulls and numeric precision. The inventory source item ID is the JSON pair `["source_code","sku"]` with the record's values. The currency ID is its base currency code.

## Source options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `base_url` | String | Required | HTTPS deployment URL before `/rest`, with optional installation path and port |
| `token` | String | Required | Bearer token for the requested API permissions |
| `store_code` | String | Empty | Optional store view code |
| `resources` | List of strings | All 21 | Unique resource names in scan order |
| `start_time` | String | Optional | Inclusive UTC start in `YYYY-MM-DDTHH:mm:ssZ`; omit for a full scan |
| `end_time` | String | Coordinator start time | Exclusive UTC end if `start_time` is configured; same format |
| `page_size` | Integer | `100` | Requested page size for paged resources, 1–1000 |
| `max_pages` | Integer | `10000` | Pages per resource, 1–100000 |
| `max_attempts` | Integer | `4` | Attempts per page including the first, 1–8 |
| `socket_timeout_ms` | Integer | `15000` | Connect and read inactivity timeout, 100–120000 ms |
| `page_deadline_ms` | Integer | `60000` | Total time to fetch and decode a page including retries, 500–600000 ms |
| `max_response_bytes` | Integer | `8388608` | Decompressed response limit, 128–33554432 bytes |

The requested time range applies to the dated resources in the table. The date-only `from_date` on sales rules represents midnight UTC and does not track modifications. An open-ended rule with a null date is included in a full scan. Full-scan resources are always read without time criteria.

For a paged resource, the source requests stable record ordering and validates page length against `total_count`. A full final page and a consistent empty page end the resource. Repeated page identities, contradictory counts, and the configured page bound cause an error. Transient requests retry within the page budget and honor valid `Retry-After` floors. Redirects are treated as errors.

Checkpoints record the resource, page and emitted row position. Restoring a partial page re-fetches and verifies it before skipping existing rows. Use the same deployment URL, store code, resource order, page size and configured window on restore. A remote collection can change during the scan. The API gives no cross-page snapshot, so shifts and repeated records are possible.

## Example

```hocon
env {
  job.mode = "BATCH"
}

source {
  AdobeCommerce {
    base_url = "https://commerce.example.com/shop"
    store_code = "default"
    token = "${commerce_token}"
    resources = ["orders", "customers", "products", "directory_currency"]
    start_time = "2026-01-01T00:00:00Z"
    end_time = "2026-01-02T00:00:00Z"
    page_size = 100
  }
}

sink {
  Console {}
}
```

<ChangeLog />
