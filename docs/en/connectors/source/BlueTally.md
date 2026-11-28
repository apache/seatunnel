import ChangeLog from '../changelog/connector-http-bluetally.md';

{/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */}

# BlueTally

Install `connector-http-bluetally` and use `BlueTally` as the source name. It reads the `assets` and `employees` arrays from the BlueTally v1 API over HTTPS. Create an API key under BlueTally **Settings > API Keys**. Multi-tenant accounts must specify `tenant_id`. The source runs a bounded scan with source parallelism 1. [Authentication and API limits](https://developer.bluetally.com/reference/getting-started-with-your-api)

## Output

The source emits one table named `records`.

| Column | Type | Description |
| --- | --- | --- |
| `resource` | STRING | `assets` or `employees`. |
| `record_id` | STRING | The exact numeric system `id` rendered as text. For assets, the separate label `asset_id` is in the payload. |
| `tenant_id` | INT | The configured tenant ID, or null when omitted. |
| `updated_at` | STRING | The update timestamp in UTC ISO 8601. Fractional precision is preserved. |
| `payload` | STRING | The complete BlueTally record encoded as JSON, including extra and nested fields. |

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `token` | STRING | Yes | | BlueTally API key sent as a Bearer token. |
| `resources` | LIST of STRING | Yes | | `assets` and/or `employees`, in scan order. |
| `start_date` | STRING | Yes | | Inclusive update time in RFC 3339 with an offset. |
| `end_date` | STRING | No | Start of the scan | Exclusive update time in RFC 3339 with an offset. |
| `tenant_id` | INT | For multi-tenant accounts | | Positive BlueTally tenant ID, sent on each request. |
| `page_size` | INT | No | 50 | BlueTally `limit`, from 1 to 1000. |
| `max_pages_per_resource` | INT | No | 1000 | Maximum successful requests per resource, including the terminal empty page. Range 1 to 10000. |
| `max_attempts` | INT | No | 4 | Total attempts per eligible GET. Range 1 to 10. |
| `request_timeout_ms` | INT | No | 30000 | Page acquisition deadline, including retries. Range 100 to 300000. |
| `max_response_bytes` | INT | No | 8388608 | Maximum decoded bytes per successful page. Range 1024 to 67108864. |
| `endpoint` | STRING | No | `https://app.bluetallyapp.com` | Custom HTTPS origin without a path, query, fragment or credentials. Requires `trust_certificate`. |
| `trust_certificate` | STRING | No | | Worker-local PEM trust anchor for a custom origin. Hostname checks stay enabled. |

## Pagination and recovery

The source requests `sort=updated_at&order=desc` and starts at offset zero for each resource. The next offset advances by the full response length. Short pages and pages whose records fall outside the output window still advance the offset. An empty array ends that resource. An out-of-order page, a repeated page, a duplicate ID in the same or adjacent page or a page limit reached before the empty response fails the job. [List assets](https://developer.bluetally.com/reference/list-assets) [List employees](https://developer.bluetally.com/reference/list-employees)

The source selects an omitted upper bound once and saves it with the resource and offset. Recovery checks the request scope and verifies any partially published page before it emits the remaining rows. The offset API provides no snapshot across records added, updated or deleted during the scan. The timestamp window selects current records and does not provide a change log or delete events.

The source respects valid `Retry-After` values within its time and attempt limits. An HTTP 429 without a positive retry delay fails the request. The connector follows no redirects and excludes credentials and response fields from diagnostics. Asset and employee payloads can contain sensitive data.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  BlueTally {
    token = ${BLUETALLY_TOKEN}
    resources = ["assets", "employees"]
    start_date = "2024-03-01T00:00:00Z"
    end_date = "2024-04-01T00:00:00Z"
  }
}
```

## Changelog

<ChangeLog />
