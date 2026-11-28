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

import ChangeLog from '../changelog/connector-http-bamboohr.md';

# BambooHR

Install `connector-http-bamboohr` and use the `BambooHR` source in a batch job with source parallelism 1. Each source instance reads one resource from the company's BambooHR v1 HTTPS API. Configure the company's subdomain and an [API key](https://documentation.bamboohr.com/docs/getting-started) with permission to read the selected resource.

| Resource | Records |
| --- | --- |
| `custom_reports_stream` | Employees in an ad-hoc custom report. Specify `custom_reports_fields`. The provider includes both active and inactive employees and can omit unknown or inaccessible fields. The [custom report API](https://documentation.bamboohr.com/reference/request-custom-report) is deprecated. |
| `employees_directory_stream` | Employee rows from the published company directory. The available fields and employees depend on directory sharing settings. |
| `meta_fields_stream` | Available employee field definitions, including custom aliases. |
| `time_off_requests_stream` | Time off requests whose absence dates overlap the inclusive API window. The source also filters on the returned `created` date when present. |
| `timesheet_entries` | Clock and hour entries in inclusive company calendar windows, with one request per calendar month or partial month. |
| `employees` | Employee detail records for internal IDs from the published directory. Specify `employee_fields` to select data. With `employee_changed_since`, this resource uses the provider's changed-ID feed instead. |

Every row has three `STRING` fields. `data` contains one JSON object and retains provider field names, nested values, numbers, and explicit nulls. Missing provider fields remain absent. The `change_type` and `last_changed` fields are null for snapshot resources. Changed-employee rows carry `Inserted`, `Updated` or `Deleted` and the provider's timestamp. For a deletion, `data` contains only the employee ID. A visible directory entry does not guarantee access to its detail record. The detail scan fails explicitly if the provider denies or cannot find that record.

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `company` | string | — | Required BambooHR company subdomain, as a single DNS label. |
| `token` | string | — | Required API key. The connector supplies it as the Basic authentication username with password `x`. |
| `resource` | string | — | Required resource name from the table above. |
| `custom_reports_fields` | string | — | Required for custom reports. Comma-separated field IDs or names, up to 400. |
| `employee_fields` | string | `firstName,lastName` | Comma-separated names, numeric field IDs or custom aliases for employee detail, up to 400. |
| `start_date` | string | Initialization time minus 30 days and 23 hours | Inclusive lower date for time off and timesheets. An explicit watermark changes how the lower bound is calculated. |
| `end_date` | string | Company date at initialization | Inclusive upper date for time off and timesheets. |
| `company_time_zone` | string | `UTC` | Java zone ID matching the company's time zone, for date conversion and validation. |
| `time_off_watermark` | string | — | Optional prior time off creation date. Re-read up to 365 days before it, subject to an explicit `start_date`. |
| `timesheet_watermark` | string | — | Optional prior timesheet date. Re-read up to 30 days before it, subject to `start_date` and provider retention. |
| `employee_changed_since` | string | — | Optional lower timestamp for [changed employee IDs](https://documentation.bamboohr.com/reference/get-changed-employee-ids). Accepts ISO 8601 with an explicit offset. |
| `connect_timeout_ms` | int | `10000` | TCP connect timeout, 1–120000 milliseconds. |
| `read_timeout_ms` | int | `30000` | Socket read timeout, 1–120000 milliseconds. |
| `request_deadline_ms` | int | `60000` | Total per-request budget including retries, 1–300000 milliseconds. |
| `max_attempts` | int | `4` | Request attempts including the first, 1–10. |
| `max_response_bytes` | int | `8388608` | Separate on-wire and decoded response size limits, 1024–33554432. |
| `max_records_per_response` | int | `20000` | Maximum records or employee IDs in a response, 1–100000. |

Date and watermark options accept YYYY-MM-DD or an ISO 8601 timestamp with an offset. The source converts timestamps to dates in `company_time_zone`. Timesheets use the provider's `date` field, or the offset `start` converted to the configured zone when `date` is absent. The earliest requested timesheet date is 364 days before the current company date. Rows with neither applicable date field stay in the provider's response window.

The window and optional change upper bound are fixed when the job starts. Changed employee details reflect the current view when the detail request runs. The service returns the latest change per employee, so this mode can be used for overlapping synchronization but does not provide every intermediate change.

Retries honor `Retry-After` within the total deadline. The connector checks the certificate and hostname, uses the current company-host v1 route, and rejects redirects. Use the same provider account and permissions when recovering a job.

A Zeta checkpoint stores positions and digests. If the rows or employee IDs already being consumed have changed before recovery, the source fails instead of making an unsafe seek. An already completed resource stays complete after recovery. A new scheduled job has a new window. Supply watermarks explicitly when an external scheduler needs overlap across separate jobs. Access controls apply to all six resources. For example, a restricted time off response can be empty even when employees have requests.

## Example

```hocon
env {
    job.mode = "BATCH"
    parallelism = 1
}
source {
    BambooHR {
        company = "example"
        token = ${BAMBOOHR_TOKEN}
        resource = "employees"
        employee_fields = "firstName,lastName,department"
    }
}
sink {
    Console {}
}
```

<ChangeLog />
