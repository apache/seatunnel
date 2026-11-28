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

import ChangeLog from '../changelog/connector-http-aws-cloudtrail.md';

# AWS CloudTrail

`AwsCloudTrail` reads the `management_events` resource through the regional AWS CloudTrail
[LookupEvents API](https://docs.aws.amazon.com/awscloudtrail/latest/APIReference/API_LookupEvents.html). Install `connector-http-aws-cloudtrail` with
`bin/install-plugin.sh`. Use a SeaTunnel Zeta batch with source parallelism 1 and credentials
authorized for `cloudtrail:LookupEvents`.

The source reads AWS account management event history. The history is regional and covers up to
90 days. Trail exports in S3, CloudTrail Lake queries, Insights and data events use different AWS interfaces.

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `access_key` | string | — | Required AWS access key ID. |
| `secret_key` | string | — | Required AWS secret access key. |
| `security_token` | string | `""` | AWS STS session token for temporary credentials. |
| `region` | string | `us-east-1` | AWS CloudTrail control-plane region. |
| `start_date` | string | Initial UTC midnight | Inclusive UTC date or offset timestamp. |
| `end_date` | string | Initial job start | Inclusive UTC date or offset timestamp. |
| `lookup_attribute_key` | string | `""` | Key for a single optional server-side filter. |
| `lookup_attribute_value` | string | `""` | Required when the filter key is set. |
| `page_size` | int | `50` | Requested records per page, 1–50. |
| `max_pages` | int | `2000` | Maximum pages for one lookup or its restore, 1–20000. |
| `max_response_bytes` | int | `8388608` | Independent wire and decoded response limits, 1024–33554432. |
| `connect_timeout_ms` | int | `10000` | TCP connect timeout, 1–300000. |
| `read_timeout_ms` | int | `15000` | TLS handshake and response inactivity timeout, 1–300000. |
| `request_timeout_ms` | int | `60000` | Total deadline for one page, including pacing and retries, 1–300000. |
| `max_attempts` | int | `3` | Maximum attempts for one page including its first request, 1–10. |

Use the documented standard control-plane endpoints for supported AWS commercial, GovCloud and
China regions. China requests use the `amazonaws.com.cn` suffix and credentials issued for that
partition. The selected region determines both the DNS endpoint and signing scope.
[AWS endpoints](https://docs.aws.amazon.com/general/latest/gr/ct.html), [AWS China endpoints](https://docs.amazonaws.cn/en_us/awscloudtrail/latest/userguide/cloudtrail-supported-regions.html)

The eight filter keys are `AccessKeyId`, `EventId`, `EventName`, `EventSource`, `ReadOnly`,
`ResourceName`, `ResourceType`, and `Username`. Values use AWS's 2000-character weighted limit.
An underscore, space, comma or newline counts as two. [Lookup attribute contract](https://docs.aws.amazon.com/awscloudtrail/latest/APIReference/API_LookupAttribute.html)

## Time bounds and recovery

Both limits are inclusive. A date such as `2026-11-01` means UTC midnight at the start of that day.
An offset timestamp uses whole seconds, for example `2026-11-01T10:30:00+08:00`. The omitted start
uses the initial job's UTC date and the omitted end uses that job's initial clock time, rounded
down to a second. The connector rejects future end times and starts older than the available history.

The source keeps the exact window and filter through all pages and retries. A short or empty
page can still have a continuation. Events are emitted newest-first. Same-second events keep
their page order. Duplicate IDs returned by AWS are preserved. The page limit
fails an incomplete lookup instead of treating it as finished.

Checkpoint state records the fixed window, opaque current page token, completed pages and a
digest for an unfinished page. On restore, completed pages are skipped. A partially emitted
page is resumed only after the full contents, ordering and continuation match its digest.
The checkpoint binds the AWS access key ID, region and query settings. A changed key ID or region
requires a new lookup. Expired continuation tokens and expired history cause a visible failure.
AWS does not expose an immutable history snapshot across separate jobs.

Each reader paces all requests and retries to stay under two sends per second. AWS quotas
apply across all clients sharing one account and region. HTTPS uses AWS Signature Version 4,
JVM trust and hostname verification. Credentials and pagination tokens are excluded from source
error messages. The current pagination token can be stored in checkpoint state.

## Output

| Field | Type | Description |
| --- | --- | --- |
| `resource` | string | `management_events`. |
| `region` | string | Region queried for the event. |
| `event_id` | nullable string | AWS's event ID. |
| `event_time` | nullable string | Exact decimal Unix seconds from `EventTime`. The JSON in `data` retains the number. |
| `cloud_trail_event` | nullable string | Original textual value of `CloudTrailEvent`. |
| `data` | string | Complete event object as JSON text, including AWS response fields such as `ReadOnly` and `Resources`. |

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  AwsCloudTrail {
    region = "us-east-1"
    access_key = ${CLOUDTRAIL_ACCESS_KEY}
    secret_key = ${CLOUDTRAIL_SECRET_KEY}
    lookup_attribute_key = "ReadOnly"
    lookup_attribute_value = "true"
  }
}
sink {
  Console {}
}
```

## Changelog

<ChangeLog />
