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

import ChangeLog from "../changelog/connector-http-campaign-monitor.md";

# Campaign Monitor

## Description

The Campaign Monitor source reads twelve collections through the v3.3 API at `https://api.createsend.com`. Use an account API key that can read the selected resources. The source uses the key in HTTP Basic authentication. Client discovery uses the account's visible client list, so the selected child records stay associated with their own client. [API authentication](https://www.campaignmonitor.com/api/v3-3/getting-started/) and [account API](https://www.campaignmonitor.com/api/v3-3/account/).

| Resource | Record key | Response |
| --- | --- | --- |
| `clients` | `ClientID` | Account list |
| `admins` | `EmailAddress` | Account list |
| `client_details` | `BasicDetails.ClientID` | One object per client |
| `segments` | `SegmentID` | Client list |
| `templates` | `TemplateID` | Client list |
| `people` | `EmailAddress` | Client list |
| `tags` | `Name` | Client list |
| `subscriber_lists` | `ListID` | Client list |
| `suppression_lists` | `EmailAddress` | Client pages |
| `sent_campaigns` | `CampaignID` | Client pages |
| `draft_campaigns` | `CampaignID` | Client list |
| `scheduled_campaigns` | `CampaignID` | Client list |

Selecting a client resource discovers its parents automatically. Select `clients` to emit the discovery records as well. For `client_details`, the source checks the returned client identifier and removes the API key field from the result. Preview, archive and content URLs stay in the output as values. [Client API](https://www.campaignmonitor.com/api/v3-3/clients/).

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| token | string | yes | | Campaign Monitor account API key. |
| resources | list | no | [] | Resource names from the table. An empty list selects all twelve. |
| start_date | string | for sent campaigns | | First included send date, in `YYYY-MM-DD` format. |
| end_date | string | no | | First excluded send date, in `YYYY-MM-DD` format. It must be later than `start_date`. |
| page_size | int | no | 1000 | Page size for sent campaigns and suppression lists, from 10 to 1,000. |
| max_pages_per_collection | int | no | 10000 | Maximum accepted pages per collection under a client. |
| max_pages_per_scan | int | no | 100000 | Maximum accepted responses across the scan. |
| max_unpaginated_items | int | no | 10000 | Maximum records in one full-list response. |
| tls_trust_certificate | string | no | JVM default trust | Optional source-local PEM or base64 DER X.509 trust anchor. Hostname checks remain active. |
| connect_timeout_ms | int | no | 10000 | TCP connection timeout in milliseconds. |
| request_timeout_ms | int | no | 30000 | Deadline for one HTTPS attempt. |
| operation_timeout_ms | int | no | 120000 | Deadline across attempts of a logical read. Must be at least `request_timeout_ms`. |
| request_interval_ms | int | no | 100 | Minimum spacing between attempts in milliseconds. |
| max_retries | int | no | 3 | Additional attempts for transient errors. |
| max_retry_delay_ms | int | no | 60000 | Maximum permitted server delay or retry backoff. |
| max_response_bytes | int | no | 8388608 | Maximum response bytes before and after gzip decompression. |

## Dates, paging and recovery

Sent campaigns use the API's date-only `sentFromDate` and `sentToDate` filters. `SentDate` is a date and time string without an offset. The connector compares its date component against the configured bounds and retains the original time text. To replay overlapping sends in a separate batch, use a previous send date as the next lower bound. The resource, client and record ID identify rows across overlapping scans. Other resources keep their own date fields as returned. [Sent campaigns](https://www.campaignmonitor.com/api/v3-3/clients/).

The two paged collections use `Results` and the page metadata from the v3.3 API. A short or empty result with more pages continues. Inconsistent metadata, invalid dates, auth errors and redirects fail the read. A transient read can be retried within the configured retry and deadline limits. If the server supplies a retry delay beyond those limits, the read fails.

On restore, the source checks the saved page digest and client scope before continuing at the saved row. The checkpoint keeps no raw records or API keys. Completed collections stay completed. An expired credential, changed date filter or changed replayed page can require a new batch. The API collections can change during a scan.

## Output

All five columns use string types.

| Column | Meaning |
| --- | --- |
| `resource` | Selected resource name. |
| `id` | Exact native record key. |
| `client_id` | Client identifier for client records and their child collections. Null for account administrators. |
| `sent_date` | Original `SentDate` text for sent campaigns. Null for the other resources. |
| `record` | Native JSON record with nested fields, nulls and exact numbers. |

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  CampaignMonitor {
    token = ${CAMPAIGN_MONITOR_TOKEN}
    resources = ["clients", "subscriber_lists", "sent_campaigns"]
    start_date = "2026-01-01"
    end_date = "2026-02-01"
  }
}
```

## Changelog

<ChangeLog />
