import ChangeLog from '../changelog/connector-http-babelforce.md';

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

# Babelforce

Install `connector-http-babelforce` and use the `Babelforce` source with parallelism 1. It reads the `calls` resource from `GET /api/v2/calls/reporting/simple` in a bounded job. Select the region holding your account. The provider accepts dual access headers and OAuth bearer authentication on this route. [Babelforce Manager API](https://apps.babelforce.com/developer-hub/manager/)

## Output

| Column | Type | Value |
| --- | --- | --- |
| `call_id` | STRING | Call ID from the response. |
| `parent_id` | STRING | Parent call ID for a child where supplied, otherwise null. |
| `date_created` | STRING | Normalized UTC creation timestamp, retaining available fractional precision. |
| `payload` | STRING | Complete UTF-8 JSON call object. Nested fields, recording references, nulls and exact numbers are preserved. |

The source emits INSERT rows. Recording references remain metadata.

## Source options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `region` | `String` | `services` | The Babelforce region. Supported values are services, us-east and ap-southeast. |
| `auth_mode` | `String` | `access_key` | Use access_key for the access ID and access token headers, or bearer for an existing OAuth token. |
| `access_key` | `String` | — | Access ID. Required in access_key mode. |
| `token` | `String` | — | Required. Access token or previously obtained OAuth bearer token. |
| `date_created_from` | `Long` | `0` | First inclusive Unix second for creation time. |
| `date_created_to` | `Long` | Current Unix second | Last inclusive Unix second, fixed at first split creation if omitted. |
| `window_parameters` | `String` | `date_created` | date_created uses the Manager OpenAPI arguments. time uses the published Go SDK arguments. |
| `page_size` | `Integer` | `102` | Maximum records per requested page. Range 1 to 102. |
| `max_pages` | `Integer` | `10000` | Maximum pages in one scan. Range 1 to 20,000. |
| `max_attempts` | `Integer` | `4` | Maximum total attempts for each retryable page. Range 1 to 10. |
| `request_timeout_ms` | `Integer` | `30000` | Overall page deadline including retries and decoding. Range 100 to 300,000. |
| `max_response_bytes` | `Integer` | `8388608` | Decoded response size limit. Range 1,024 to 67,108,864. |
| `endpoint` | `String` | Regional HTTPS URL | Optional explicitly trusted custom HTTPS origin paired with trust_certificate. |
| `trust_certificate` | `String` | — | Worker-local PEM trust anchor for a custom endpoint. Hostname validation remains active. |

## Date window and recovery

- The default `window_parameters = date_created` sends `dateCreated.start` and `dateCreated.end`. Choose `time` for deployments using the published SDK's `time.start` and `time.end` parameters. All four query fields are Unix seconds. The source checks each returned call's creation second against the configured range. An offset timestamp is normalized to UTC. Offset-free timestamps are interpreted as UTC. [Babelforce Manager specification](https://apps.babelforce.com/developer-hub/manager/) [Babelforce SDK](https://github.com/babelforce/manager-sdk-go)
- The range end and page number stay fixed in the relevant checkpoint. The connector follows the reported total page count, including empty intermediate pages, and checks any supplied page identity, total records and continuation. A first response without a page count is a single page if its other metadata agrees. The connector constructs requests locally and rejects a continuation that conflicts with the expected route or position.
- A partial-page restore verifies the re-fetched response against its stored digest before publishing the remaining rows. Changed call data or read scope can require a new scan. Call order can still change between provider pages, and creation-time reads do not capture every later update or purge. Reconcile overlapping scans downstream using the source identity and call ID.

## Example

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}
source {
  Babelforce {
    region = "services"
    access_key = ${BABELFORCE_ACCESS_ID}
    token = ${BABELFORCE_ACCESS_TOKEN}
    date_created_from = 1730592000
    plugin_output = "calls"
  }
}
sink {
  Console {
    plugin_input = "calls"
  }
}
```

<ChangeLog />
