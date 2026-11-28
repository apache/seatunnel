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

import ChangeLog from '../changelog/connector-http-apptivo.md';

# Apptivo

Install `connector-http-apptivo` and use the `Apptivo` source in a batch job. Obtain your firm's API key and access key under **Business Settings → API Access**. The connector sends both as encoded query parameters to the configured HTTPS origin. Use the Apptivo API origin unless your deployment provides a different trusted endpoint. [Apptivo developer guide](https://www.apptivo.com/developer-api/) · [API access and authentication](https://www.apptivo.com/developer-api/getting-started/)

## Resources

| Resource | DAO action | Row ID |
| --- | --- | --- |
| `customers` | `getAll` | `customerId` |
| `contacts` | `getAll` | `contactId` |
| `cases` | `getConfigData` | `case-config` |
| `leads` | `getAll` | `id` |
| `opportunities` | `getAll` | `opportunityId` |

The four `getAll` resources read batches from a zero-based offset. If Apptivo returns pagination counters, the connector checks that they agree with the current page. `cases` emits one configuration object containing settings such as statuses, types, priorities and assignees. [Customer listing](https://www.apptivo.com/developer-api/customers-api-reference/getallcustomers/) · [Case settings](https://www.apptivo.com/developer-api/cases-api-reference/getcasesconfigdata/)

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `token` | string | required | Apptivo business API key |
| `access_key` | string | required | Apptivo business access key |
| `firm_id` | string | none | Expected positive Apptivo firm ID. Supply it to bind a checkpoint to a particular firm. |
| `resources` | list of string | all five | Resource names in read order |
| `api_origin` | string | `https://api.apptivo.com` | Fixed HTTPS API origin without path or query |
| `tls_trust_certificate` | string | JVM trust | Optional trusted X.509 certificate for this source in PEM or base64 DER form. HTTPS hostname verification remains active. |
| `page_size` | integer | 100 | Requested list records per page, from 1 to 100 |
| `max_pages_per_resource` | integer | 1000 | Maximum pages for one collection |
| `request_timeout_ms` | integer | 30000 | Deadline for one full HTTPS attempt |
| `operation_timeout_ms` | integer | 120000 | Deadline across attempts for one request |
| `min_request_interval_ms` | integer | 100 | Minimum interval between HTTPS attempts |
| `max_retries` | integer | 3 | Additional attempts for transient statuses or GET connection failures |
| `max_response_bytes` | integer | 4194304 | Maximum transferred and decoded JSON bytes |

## Output and recovery

The source emits three string columns, `resource`, `id` and `record`. `record` contains the complete Apptivo JSON object, including nested and custom fields. Provider `FAILURE` responses fail the job with a sanitized diagnostic.

One active source reader keeps the resource order, offset, accepted row position, page fingerprint and an available provider firm ID in checkpoints. When restoring, use credentials for the same firm. The source checks `firmId` when Apptivo supplies it and verifies saved page content before resuming an in-page offset. A changed page fails the restore. New jobs scan the selected collections afresh. Apptivo may update records during a scan, and delivery to the destination depends on the sink.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Apptivo {
    token = ${APPTIVO_API_KEY}
    access_key = ${APPTIVO_ACCESS_KEY}
    resources = ["customers", "contacts", "cases", "leads", "opportunities"]
  }
}

sink {
  Console {}
}
```

Set both credentials through your job configuration's secret injection mechanism.

<ChangeLog />
