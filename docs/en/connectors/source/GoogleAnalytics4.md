<!--
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements. See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License. You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

import ChangeLog from '../changelog/connector-google-analytics4.md';

# GoogleAnalytics4

## Description

Read one Google Analytics 4 property report through the Data API v1beta `runReport` REST method.
This source reads aggregated reporting rows, **not raw events, CDC, or an immutable snapshot**.

## Supported Connector Version

Google Analytics 4, Data API v1beta. Universal Analytics is not supported.
The connector artifact bundles its Google authentication and HTTP dependencies; no separate driver installation is required.

## Supported Engines

SeaTunnel Zeta, Flink and Spark.

## Key Features

- Batch only, source parallelism must be 1.
- One property and one inclusive absolute date range in the property's time zone.
- Explicit dimensions, metrics, Google metric types and matching SeaTunnel schema.
- Bounded page requests, response sizes, deadlines and transient-error retries.
- Service-account OAuth refresh; no credentials are needed for the isolated HTTP test fixture.

## Consistency and Recovery

All dimensions are ordered ascending using Google's `ALPHANUMERIC` ordering, in configured order.
The next offset advances by the number of rows actually returned, including short nonempty pages.
The source rejects changing `rowCount`, duplicate/out-of-order dimension tuples, empty pages before
the reported end, and mismatched headers, types or row widths. It never truncates at `max_report_rows`.

These checks **do not prove completeness or snapshot consistency**. GA4 can update historical reports
between requests without changing their count or ordering. Rows can still move across offsets, and
metric values can change. Absolute dates freeze the request, not the service's data. Prefer a settled
historical range and reconcile results with the property when consistency matters.

A single report is read while the common single-split reader holds the checkpoint lock. There are no
page checkpoints or resumable offsets. Recovery replays the whole report from offset zero with the
same absolute dates. The connector does not restart internally on a changing report. Engine/job
restart limits remain the operator's responsibility. Previously emitted rows can be replayed, and a
late page failure can leave partial output in a nontransactional sink. Use a staging/replacement
workflow or a sink suited to your recovery requirements; do not treat failed-job output as complete.

The source rejects reports whose metadata indicates sampling (`samplingMetadatas`), thresholding
(`subjectToThresholding`), other-row data loss (`dataLossFromOtherRow`), active metric restrictions,
or a nonempty `emptyReason`. Thresholding is rejected even when it may not have removed any rows.
There is no option to silently accept these limitations. Missing optional metadata is accepted, and
absence of a warning is not a complete-data guarantee. A literal dimension value `(other)` alone is
not interpreted as the metadata signal.

## Authentication

Enable the Google Analytics Data API in the service-account project and grant the service-account
email read access (for example, Viewer) to the GA4 property. Supply a trusted service-account JSON
file on every worker at the configured path. Only service-account key files are supported in V1:
there is no Application Default Credentials, raw token, user refresh-token or workload federation mode.

The connector uses Google Auth Library with the `analytics.readonly` scope. It refreshes before
expiry and once on HTTP 401 per page. Token exchange uses the fixed
`https://oauth2.googleapis.com/token` endpoint and is bounded by the HTTP/report deadline.
Malformed keys, nonstandard token URIs and refresh failures fail without including keys, tokens,
HTTP bodies or underlying authentication exceptions in connector diagnostics. Keep key files
restricted to the worker account. Do not put key contents in the job configuration.

`emulator_url` is an explicit **unauthenticated test-fixture origin**, not an official GA4 emulator.
It is mutually exclusive with the key file and supports only a non-Google HTTP origin, without
userinfo, query, fragment or path. Never use this mode for production GA4 access. Production requests
use the fixed HTTPS Google endpoint. Redirects, cookies and implicit HTTP retries are disabled.

## Source Options

| Name | Type | Required | Default | Description |
| :--- | :--- | :--- | :--- | :--- |
| property_id | string | yes | - | One numeric GA4 property ID, not a measurement ID. |
| start_date | string | yes | - | Inclusive absolute calendar date, `yyyy-MM-dd`. Relative dates are rejected. |
| end_date | string | yes | - | Inclusive absolute date, not before start_date. |
| dimensions | array | no | [] | Ordered unique dimension API names, at most 9. |
| metrics | array | yes | - | Ordered unique metric API names, 1 to 10; must not overlap dimensions. |
| metric_types | array | yes | - | Exact Google MetricType enum per metric, in the same order. |
| schema | config | yes | - | Exactly all dimensions followed by all metrics, with names, order and types matching. |
| service_account_key_file | string | conditional | - | Trusted service-account file on workers; at most 1 MiB. Exactly one authentication option is required. |
| emulator_url | string | conditional | - | Unauthenticated local/isolated HTTP fixture origin, mutually exclusive with key file. |
| page_size | int | no | 1000 | Requested rows per page, 1 to 10000. This connector intentionally uses a smaller bound than Google's API maximum. |
| max_report_rows | int | no | 1000000 | Fail if reported row count exceeds this bound; range 1 to 10000000. |
| max_response_bytes | int | no | 4194304 | Fail on a larger page response; range 1024 to 16777216. Oversize responses are not retried. |
| request_timeout_ms | int | no | 30000 | HTTP request deadline, including body transfer; 100 to 120000 ms. Synchronous JVM/platform DNS resolution is not guaranteed to be interruptible. |
| report_timeout_ms | int | no | 600000 | Whole report deadline; 100 to 3600000 ms. Applies to HTTP, auth and retry waits; checked between collected rows. It cannot interrupt synchronous DNS resolution or a downstream sink's blocking collect call. |
| max_retries | int | no | 3 | Additional attempts per page for transient transport errors or HTTP 429/500/502/503/504; 0 to 5. |
| max_retry_wait_ms | int | no | 30000 | Upper bound on a retry wait; 1000 to 120000 ms. |

Retries use bounded exponential backoff with jitter and honor `Retry-After` seconds or HTTP dates.
If the server requests a longer wait than configured, the job fails instead of retrying early.
Invalid requests, forbidden access, schema errors, oversized responses and auth refresh errors are
not retried. Exhausted token/server-error quota reported on an unfinished page fails the job before
another request. No background polling waits for a daily/hourly quota reset.

Memory is bounded per page, not for the whole report. JSON parsing and typed rows require more memory
than the byte limit itself. Tune both page size and byte limit for the worker heap. Close/cancellation
aborts active HTTP requests and wakes retry waits. HTTP connection and body deadlines also bound stalled
responses. Report timeout does not control engine restarts or downstream backpressure.
Apache HttpClient resolves hostnames synchronously before socket connection. JVM/platform DNS lookup
can block outside connect/socket timeouts, and aborting a request does not guarantee interruption of
that lookup. Request/report deadlines and cancellation are therefore not hard wall-clock bounds while
DNS resolution is blocked; this also applies to OAuth token requests.

## Data Types

Dimension values use STRING, including empty strings and date-like dimension values; the source does
not reinterpret them as dates. `TYPE_INTEGER` metrics require BIGINT and are checked for 64-bit overflow.
`TYPE_FLOAT`, `TYPE_SECONDS`, `TYPE_MILLISECONDS`, `TYPE_MINUTES`, `TYPE_HOURS`, `TYPE_STANDARD`,
`TYPE_CURRENCY`, `TYPE_FEET`, `TYPE_MILES`, `TYPE_METERS` and `TYPE_KILOMETERS` require DOUBLE.
Nonfinite/invalid values fail. DOUBLE has floating-point precision, including currency metrics;
this is not an exact-decimal money representation. Exact Google enum types are checked on every page
so changes in metric units do not pass as compatible DOUBLE values.

Check the property's supported dimensions/metrics and their compatibility before configuring a report.
API incompatibility errors fail with the HTTP status and redacted response details. V1 does not support
filters, expressions, cohorts, comparisons, multiple date ranges, realtime reports or metric aggregations.
No network call is made to discover or mutate the schema during factory construction.

## Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}
source {
  GoogleAnalytics4 {
    property_id = "123456789"
    service_account_key_file = "/run/secrets/ga4-service-account.json"
    start_date = "2024-01-01"
    end_date = "2024-01-31"
    dimensions = ["country"]
    metrics = ["activeUsers", "purchaseRevenue"]
    metric_types = ["TYPE_INTEGER", "TYPE_CURRENCY"]
    schema {
      fields {
        country = string
        activeUsers = bigint
        purchaseRevenue = double
      }
    }
  }
}
sink {
  Console {}
}
```

For an isolated mock test, replace `service_account_key_file` with `emulator_url = "http://fixture:1080"`.
Never configure both.

## Verification

The connector includes deterministic HTTP fixture tests and a mock-server engine E2E test.
Authentication tests use generated RSA credentials and a mock OAuth token exchange. These tests do not
verify a live GA4 property. Before a production handoff, verify property access, metric compatibility,
pagination, quota behavior and reporting limitations using authorized real-property credentials.
No such credentials are bundled with the connector.

## References

- [runReport REST API](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/properties/runReport)
- [Response metadata](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/ResponseMetaData)
- [Metric types](https://developers.google.com/analytics/devguides/reporting/data/v1/rest/v1beta/MetricType)
- [Quotas](https://developers.google.com/analytics/devguides/reporting/data/v1/quotas)
- [API quickstart and property access](https://developers.google.com/analytics/devguides/reporting/data/v1/quickstart-client-libraries)

## Changelog

<ChangeLog />
