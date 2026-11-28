import ChangeLog from '../changelog/connector-http-aviationstack.md';

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

# Aviationstack

Install `connector-http-aviationstack` and set the source name to `Aviationstack`. It reads selected Aviationstack reference catalogs in a bounded job with parallelism 1. Each page uses a GET over HTTPS with the account's access key in the provider's query parameter. Choose only the catalogs you need because every successful page counts toward the provider's request allowance. [Aviationstack authentication and errors](https://apilayer.portal.swaggerhub.com/aviationstack/docs/getting-started)

## Catalogs

| Resource | API path | Contents |
| --- | --- | --- |
| `airports` | `/v1/airports` | Airports and location information. |
| `airlines` | `/v1/airlines` | Airline names, codes and fleet metadata. |
| `airplanes` | `/v1/airplanes` | Aircraft registration and delivery information. |
| `aircraft_types` | `/v1/aircraft_types` | Aircraft types and model identifiers. |
| `cities` | `/v1/cities` | Cities with aviation identifiers. |
| `countries` | `/v1/countries` | Countries and country codes. |
| `taxes` | `/v1/taxes` | Aviation tax codes and names. |

## Output

| Column | Type | Value |
| --- | --- | --- |
| `resource` | STRING | Catalog name. |
| `record_id` | STRING | Provider `id` as text. Use the pair of resource and ID to identify a row across catalogs. |
| `payload` | STRING | Full provider object as UTF-8 JSON, including nulls, nested fields and exact numeric values. |

## Source options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `access_key` | `String` | — | Required. API key from the Aviationstack dashboard. |
| `resources` | `List<String>` | — | Required. Unique catalog names read in the configured order. |
| `start_date` | `String` | — | Inclusive RFC 3339 lower bound for airplane delivery time. |
| `end_date` | `String` | — | Inclusive RFC 3339 upper bound for airplane delivery time. With only a start date, defaults to the time the scan split was created. |
| `page_size` | `Integer` | `100` | Requested records per page. Range 1 to 100. |
| `max_pages_per_resource` | `Integer` | `10000` | Maximum provider pages per catalog. Range 1 to 20,000. |
| `max_attempts` | `Integer` | `4` | Total attempts for a retryable page. Range 1 to 10. |
| `request_timeout_ms` | `Integer` | `30000` | Time budget for a page including body consumption and retries. Range 100 to 300,000. |
| `max_response_bytes` | `Integer` | `8388608` | Decoded response byte limit. Range 1,024 to 67,108,864. |
| `endpoint` | `String` | `https://api.aviationstack.com` | Optional HTTPS origin. Use the root path and pair with `trust_certificate`. |
| `trust_certificate` | `String` | — | Worker-local PEM trust anchor for the configured custom endpoint. Hostname verification remains enabled. |

## Delivery dates, pagination and recovery

- The optional date window applies only to `airplanes`. Dates are compared in UTC, inclusive on both ends. Date-only values represent midnight UTC. The connector accepts offset timestamps, including older payloads with a space separator or an offset without a colon. A configured window excludes records without a delivery date. An invalid non-null date fails the windowed scan. Without a window the source preserves all date values as returned.
- The API receives only the access key and pagination arguments. A window still reads every raw airplane page, including pages that yield no eligible rows. The delivery date represents a historical event and does not provide an update stream.
- Offset advances by the provider's raw page count after checking page metadata. Repeated pages, contradictory counts and changed totals stop the job. The provider's offset API can still change the order of records while the total stays constant. A scan therefore has no vendor snapshot guarantee.
- A checkpoint stores the route order, offset, any frozen time bound and the emitted page position. A partially published page is fetched again and verified before continuing the remainder. Changes to its payload or the read scope fail recovery. Apply downstream deduplication where required.
- Account quota exhaustion or a plan denial stops the whole scan. Recognized burst throttling and transient failures can retry within the deadline. The provider can charge overage for successful requests according to the subscription. [Catalog endpoint specification](https://apilayer.portal.swaggerhub.com/aviationstack/docs/api-documentation) [Quota and billing FAQ](https://aviationstack.com/faq)

## Example

The example reads airports and airplane records delivered from January 1, 2024 through the start of the scan.

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}
source {
  Aviationstack {
    resources = ["airports", "airplanes"]
    access_key = ${AVIATIONSTACK_ACCESS_KEY}
    start_date = "2024-01-01T00:00:00Z"
    plugin_output = "aviation_catalog"
  }
}
sink {
  Console {
    plugin_input = "aviation_catalog"
  }
}
```

<ChangeLog />
