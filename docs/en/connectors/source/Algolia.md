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

import ChangeLog from '../changelog/connector-http-algolia.md';

# Algolia

Install `connector-http-algolia` and configure `Algolia` in a batch job. Use an API key with the permissions required by the selected resources. The source uses the Algolia Search REST API over verified HTTPS. It sends the application ID and key as request headers, and it rejects redirects. [Algolia Search API](https://www.algolia.com/doc/rest-api/search)

## Resources

| Resource | REST operation | ACL |
| --- | --- | --- |
| `indices` | GET `/1/indexes` | `listIndexes` |
| `indexes_query` | POST `/1/indexes/{index}/query` | `search` |
| `available_languages` | GET `/1/dictionaries/*/languages` | `settings` |
| `logs` | GET `/1/logs` | `logs` |
| `indexes_settings` | GET `/1/indexes/{index}/settings` | `settings` |

The default selection includes all five resources. Query and settings export discover every accessible index, including indices outside the date window or when `indices` is deselected. Each row has `resource STRING`, `id STRING`, nullable `parent_id STRING` and `record STRING`. The ID uses the index name, search object ID, or log SHA-1 as applicable. The language map is one row with ID `languages`, including an empty object. Settings use the corresponding index name as both ID and parent ID. Search hits keep the index as parent ID and preserve ranking and highlight data when Algolia returns them. Logs export the provider payload, which may contain request or response details. [Index listing](https://www.algolia.com/doc/rest-api/search/list-indices) · [Languages](https://www.algolia.com/doc/rest-api/search/get-dictionary-languages)

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `application_id` | String | Required | Algolia application ID |
| `token` | String | Required | Algolia API key |
| `start_date` | String | Required | Inclusive UTC timestamp for index updates and logs |
| `base_url` | String | `https://<application_id>.algolia.net` | Optional HTTPS origin override |
| `end_date` | String | Job start | Exclusive UTC scan cutoff |
| `resources` | List of strings | All | Unique resource names from the table |
| `search_query` | String | `hitsPerPage=2&getRankingInfo=1` | Form-encoded parameters passed as JSON `params` |
| `indices_page_size` | Integer | `100` | Indices per page, 1–1000 |
| `logs_page_size` | Integer | `100` | Log entries per request, 1–1000 |
| `max_pages` | Integer | `10000` | Page ceiling per collection or index search, 1–20000 |
| `request_interval_ms` | Integer | `25` | Minimum spacing between requests, 10–60000 ms |
| `max_attempts` | Integer | `4` | Read attempts including the first, 1–8 |
| `socket_timeout_ms` | Integer | `15000` | Connection/read idle timeout, 100–120000 ms |
| `page_deadline_ms` | Integer | `60000` | Overall page deadline including retries, 500–600000 ms |
| `max_response_bytes` | Integer | `8388608` | Maximum decompressed bytes, 1024–33554432 |

## Query scope and recovery

The source sends `search_query` in the search request’s JSON `params` field. Without a `page` or `offset`, it reads numbered pages starting at zero and checks the reported hit count. A server cap, approximate count or incomplete page can fail the job. A failure on a later page can occur after earlier rows were emitted. An explicit `page` reads a single page. An explicit `offset` with optional `length` reads a single window. These explicit requests use the server’s accessible result range. The connector leaves index settings unchanged. [Search an index](https://www.algolia.com/doc/rest-api/search/search-single-index) · [Pagination limit](https://www.algolia.com/doc/api-reference/api-parameters/paginationLimitedTo)

Index updates and log entries are filtered locally from `start_date` through the exclusive cutoff. Use UTC timestamps such as `2026-11-27T00:00:00Z` or `2026-11-27T00:00:00.125Z`. Logs come from the configured origin and only cover records the service still returns. Algolia retains the last seven days and at most 1,000 logged requests per server. The source applies that horizon and does not aggregate across servers. [Log entries](https://www.algolia.com/doc/rest-api/search/get-logs)

The source preserves its cutoff, resource, parent and page positions on restore. It validates partially emitted pages before skipping previous rows. Keep the application, origin, resource selection, date bounds, query and page sizes the same when restoring. The key may rotate. API collections can change between requests, so a fresh job uses its explicitly configured date window rather than an automatically advanced watermark.

## Example

```hocon
env {
  job.mode = "BATCH"
}

source {
  Algolia {
    application_id = "${algolia_application_id}"
    token = "${algolia_token}"
    start_date = "2026-11-27T00:00:00Z"
    resources = ["indices", "indexes_query"]
    search_query = "query=desk&hitsPerPage=100&page=0"
  }
}

sink {
  Console {}
}
```

<ChangeLog />
