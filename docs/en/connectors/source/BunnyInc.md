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

import ChangeLog from '../changelog/connector-http-bunny-inc.md';

# BunnyInc

Install `connector-http-bunny-inc` to export data from a Bunny, Inc. billing workspace. Generate a Bunny API client access token with read access to the chosen data. Each source block reads one resource in batch mode at source parallelism `1`. [Bunny API access and authentication](https://docs.bunny.com/developer/using-the-graphql-api/api)

## Resources

| `resource` | GraphQL connection | Records |
| --- | --- | --- |
| `plans` | `plans` | Product plans |
| `quotes` | `quotes` | Quotes |
| `tenants` | `tenants` | Tenant records |
| `accounts` | `accounts` | Billing accounts |
| `contacts` | `contacts` | Account contacts |
| `entities` | `entities` | Billing entities |
| `invoices` | `invoices` | Issued invoices |
| `payments` | `payments` | Payments |
| `products` | `products` | Products |
| `invoiceItems` | `invoiceItems` | Invoice line items |
| `transactions` | `transactions` | Billing transactions |
| `quote_charges` | `quoteCharges` | Quote charge lines |
| `subscriptions` | `subscriptions` | Subscriptions |
| `accountBalances` | `accountBalances` | Account balances |
| `subscriptionCharges` | `subscriptionCharges` | Subscription charge lines |

The source reads the billing fields for these queries, including monetary amounts, charge quantities, timestamps and nullable values. Subscription `cancellationDate` is projected as `cancelationDate` for compatibility with existing export field names. [Bunny GraphQL query reference](https://docs.bunny.com/developer/api-reference/queries)

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `subdomain` | string | Required | One DNS label before `.bunny.com`, such as `my-workspace`. The source lowercases the label and builds `https://<subdomain>.bunny.com/graphql`. |
| `token` | string | Required | Bunny access token for the selected workspace. |
| `resource` | string | Required | One of the resources in the table. |
| `page_size` | int | `10` | Items requested per cursor page, from 1 to 10. |
| `max_pages` | int | `2000` | Maximum number of cursor pages, including empty pages. Range 1–20000. |
| `connect_timeout_ms` | int | `10000` | TCP connect timeout in milliseconds. Range 1–120000. |
| `read_timeout_ms` | int | `30000` | Socket read timeout in milliseconds. Range 1–120000. |
| `request_deadline_ms` | int | `60000` | Total per-request deadline for transport and retries, in milliseconds. Range 1–300000. |
| `max_attempts` | int | `4` | Maximum attempts for a retryable response, including the first. Range 1–10. |
| `max_response_bytes` | int | `8388608` | Maximum encoded and decoded response size in bytes. Range 1024–33554432. |
| `tls_trust_certificate` | string | JVM trust store | Optional engine-local PEM or DER file containing X.509 trust anchors. The source still verifies the workspace hostname. |

## Output and recovery

| String column | Value |
| --- | --- |
| `entity_id` | Bunny object ID as an opaque string when present, otherwise null. |
| `data` | Full received node as JSON with nested values, exact numeric values and original decimal scale. |

The source performs a full traversal of the selected connection using read-only queries sent by HTTPS POST. Pagination follows `pageInfo.hasNextPage` and passes the original `endCursor` as a JSON variable. Empty pages can carry a continuation. A missing or repeated cursor fails explicitly. The Bearer credential is scoped to the selected host. HTTP redirects and GraphQL error envelopes, including HTTP 200 responses carrying partial data and errors, are rejected. Retry waits honor `Retry-After` within the request deadline.

Checkpoint state holds the active cursor, bounded cursor digests and the row offset for a partly emitted page. A refetched page must match its saved record order, content and continuation before the source resumes. The access token and billing rows are absent from split state. The vendor API can change as new billing activity arrives and does not guarantee an immutable snapshot.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  BunnyInc {
    subdomain = "my-workspace"
    token = ${BUNNY_ACCESS_TOKEN}
    resource = "invoiceItems"
  }
}
sink {
  Console {}
}
```

<ChangeLog />
