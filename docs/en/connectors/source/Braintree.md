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

import ChangeLog from "../changelog/connector-http-braintree.md";

# Braintree

> PayPal Braintree source connector

## Description

Run a bounded batch read of a Braintree merchant in Production or Sandbox. Use the gateway merchant ID and an API key pair with access to the selected resources. The connector sends read-only v6 XML requests to the matching Braintree gateway. Search POSTs retrieve records or IDs.

## Resources

| Resource | Read method | Cursor |
| --- | --- | --- |
| `merchant_accounts` | Gateway collection pages | |
| `customers` | Search IDs, then fetch records in batches | `created_at` |
| `discounts` | Catalog collection | |
| `plans` | Catalog collection | |
| `transactions` | Search IDs, then fetch records in batches | `created_at` |
| `subscriptions` | Search IDs, then fetch records in batches | `created_at` |
| `disputes` | Gateway search pages | `received_date` |

Select resources by name or omit the list to read all seven. A single active reader uses the configured resource order. Transactions use the provider's 50,000-ID limit. Other searched resources use the 10,000-result limit. When a search reaches its limit, the source divides its date range before accepting records. If an individual minute or dispute date still reaches the limit, the job reports an incomplete search. Provider permissions and data-retention rules also apply. [Braintree search limits and changing results](https://developer.paypal.com/braintree/docs/reference/general/searching/search-results/java).

Customers, transactions and subscriptions use inclusive RFC3339 bounds. Braintree includes the full minute containing the supplied upper timestamp. The connector queries disjoint UTC minute ranges, then filters returned timestamps against the exact configured bounds. A dispute exposes the received date at daily precision, so its bounds are inclusive UTC calendar dates. [Braintree search range behavior](https://developer.paypal.com/braintree/docs/reference/general/searching/search-fields/java/).

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| environment | string | yes |  | Production or Sandbox. |
| merchant_id | string | yes |  | Gateway merchant identifier. |
| access_key | string | yes |  | Braintree public API key. |
| secret_key | string | yes |  | Braintree private API key. |
| start_date | string | no |  | Inclusive RFC3339 start. Required for searched resources. |
| end_date | string | no | scan start | Inclusive RFC3339 cutoff. Omit to freeze the current instant at scan start. |
| tls_trust_certificate | string | no | JVM defaults | Source-local optional PEM or base64 DER X.509 trust anchor. |
| resources | list | no | [] | Selected output resources. An empty list selects all seven. |
| connect_timeout_ms | int | no | 10000 | TCP connect timeout. |
| request_timeout_ms | int | no | 30000 | Deadline for one HTTPS attempt. |
| operation_timeout_ms | int | no | 60000 | Deadline across all attempts of one read. |
| max_retries | int | no | 3 | Maximum eligible read retries. |
| max_retry_delay_ms | int | no | 30000 | Maximum Retry-After or backoff delay. |
| max_response_bytes | int | no | 8388608 | Largest XML response before and after decompression. |
| max_xml_depth | int | no | 64 | Maximum nested XML element depth. |
| max_xml_nodes | int | no | 500000 | Maximum XML element count. |
| max_pages | int | no | 10000 | Maximum list pages or ID batches per resource. |
| max_search_windows | int | no | 10000 | Maximum search windows per searched resource, including subdivisions. |
| search_window_days | int | no | 31 | Days in each initial search window. |
| search_result_limit | int | no | 50000 | Subdivide at this result count or the lower provider cap. |
| max_ids_per_request | int | no | 1000 | Maximum requested IDs even when the provider offers a larger page size. |

## Output and checkpoints

All four output columns are strings. `resource` identifies the selected resource, `id` keeps the exact opaque ID, `cursor` is the UTC timestamp or date for searches and null for catalogs, and `record` contains a JSON object.

The output uses snake_case keys and preserves nested objects, arrays, nulls, booleans, identifiers and exact decimal values from the XML. Recognized date/time fields use ISO representations. Braintree account, transaction and customer details use the corresponding SDK-style field names, including `business_details`, `billing_details` and `payment_methods`. Additional fields inside records are retained. The connector limits XML bytes, depth and node count, and disables document type and external entity resolution.

Checkpoint recovery keeps the original end time, completed resources, page or planned ID batch and its accepted row position. A replayed page or batch must match the saved content digest before remaining rows can be emitted. Braintree can omit deleted records or records that no longer match a search after returning their IDs. The source skips absent IDs and continues through the remaining planned batches. A changing source collection can affect a batch read. For a separate job, configure the next date range explicitly.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Braintree {
    environment = "Sandbox"
    merchant_id = "your-merchant"
    access_key = ${BRAINTREE_PUBLIC_KEY}
    secret_key = ${BRAINTREE_PRIVATE_KEY}
    resources = ["plans", "transactions", "disputes"]
    start_date = "2025-01-01T00:00:00Z"
  }
}
```

## Changelog

<ChangeLog />
