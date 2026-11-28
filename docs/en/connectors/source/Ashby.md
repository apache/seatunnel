import ChangeLog from '../changelog/connector-http-ashby.md';

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

# Ashby

Read one Ashby recruiting resource per source with parallelism 1. Install `connector-http-ashby` and use the `Ashby` source identifier. Give the API key the read permission listed for your resource. SeaTunnel sends the key as the Basic authentication username with an empty password over TLS. [Ashby authentication](https://developers.ashbyhq.com/reference/authentication)

## Resources

| Resource | Ashby POST path | Read permission | Continuation |
| --- | --- | --- | --- |
| `application_criteria_evaluations` | `/application.listCriteriaEvaluations` | `candidatesRead` | Application pages, then child cursor |
| `application_feedback` | `/applicationFeedback.list` | `candidatesRead` | Cursor and sync token |
| `application_history` | `/application.listHistory` | `candidatesRead` | Application sync, then child cursor |
| `applications` | `/application.list` | `candidatesRead` | Cursor and sync token |
| `approvals` | `/approval.list` | `approvalsRead` | Cursor and sync token |
| `archive_reasons` | `/archiveReason.list` | `hiringProcessMetadataRead` | Single response |
| `candidate_tags` | `/candidateTag.list` | `hiringProcessMetadataRead` | Cursor and sync token |
| `candidates` | `/candidate.list` | `candidatesRead` | Cursor and sync token |
| `custom_fields` | `/customField.list` | `hiringProcessMetadataRead` | Cursor and sync token |
| `departments` | `/department.list` | `organizationRead` | Cursor and sync token |
| `feedback_form_definitions` | `/feedbackFormDefinition.list` | `feedbackFormDefinitionsRead` or `hiringProcessMetadataRead` | Cursor and sync token |
| `interview_plans` | `/interviewPlan.list` | `interviewsRead` | Cursor and sync token |
| `interview_schedules` | `/interviewSchedule.list` | `interviewsRead` | Cursor and sync token |
| `interview_stage_groups` | `/interviewStageGroup.list` | `interviewsRead` | Single response |
| `interview_stages` | `/interviewStage.list` | `interviewsRead` | Interview plan pages, then one stage response per plan |
| `interviewer_pools` | `/interviewerPool.list` | `hiringProcessMetadataRead` | Cursor and sync token |
| `interviews` | `/interview.list` | `interviewsRead` | Cursor and sync token |
| `job_boards` | `/jobBoard.list` | `jobsRead` | Single response |
| `job_postings` | `/jobPosting.list` | `jobsRead` | Single response |
| `job_templates` | `/jobTemplate.list` | `jobsRead` | Cursor and sync token |
| `jobs` | `/job.list` | `jobsRead` | Cursor and sync token |
| `locations` | `/location.list` | `organizationRead` | Cursor and sync token |
| `offers` | `/offer.list` | `offersRead` | Cursor and sync token |
| `openings` | `/opening.list` | `jobsRead` | Cursor and sync token |
| `projects` | `/project.list` | `candidatesRead` or `projectsRead` | Cursor and sync token |
| `source_tracking_links` | `/sourceTrackingLink.list` | `hiringProcessMetadataRead` | Cursor and sync token |
| `sources` | `/source.list` | `hiringProcessMetadataRead` | Single response |
| `survey_form_definitions` | `/surveyFormDefinition.list` | `hiringProcessMetadataRead` | Cursor and sync token |
| `users` | `/user.list` | `organizationRead` | Cursor and sync token |

## Output

Each result is an INSERT row. Keep the original provider object in `payload` when downstream jobs need candidate fields, exact numeric values, nulls or nested records. Archive and deletion fields are carried through when Ashby returns them. Determine downstream merges or removals from those fields and your data policy.

| Column | Type | Value |
| --- | --- | --- |
| `resource` | STRING | Configured resource name. |
| `record_id` | STRING | Provider result ID. Criteria evaluation IDs are qualified as `applicationId/id`. |
| `parent_id` | STRING | Application or interview plan ID for child records. Null for direct records. |
| `payload` | STRING | UTF-8 JSON result object. History adds application ID, status and timestamps. Criteria evaluations add their application ID. Interview stages add the plan's archive flag. |

## Source options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `resource` | String | — | Required. Choose one resource from the table. |
| `token` | String | — | Required. API key with the read permission for the selected resource. |
| `start_date` | String | — | Optional RFC 3339 start with at most millisecond precision. Applies to applications, application feedback, interview schedules and application history. |
| `sync.token` | String | — | Optional final sync token previously obtained for the same account, resource and filters. For application history it is an application list token. In JSON use a `sync` object with a `token` field. |
| `poll_interval_ms` | Integer | `0` | Zero runs one pass. A value from 100 to 86,400,000 enables repeated sync passes for native token resources and application history. |
| `page_size` | Integer | `100` | Requested page size for methods with a limit. Range 1 to 100. |
| `max_requests_per_pass` | Integer | `100000` | Logical request cap per pass, including discovery and child pages. Range 1 to 1,000,000. |
| `application_child_interval_ms` | Integer | `600` | Minimum interval between history or criteria evaluation HTTP attempts in one reader. Range 0 to 30,000. |
| `max_response_bytes` | Integer | `8388608` | Maximum decoded bytes per successful response. Range 1,024 to 67,108,864. |
| `max_attempts` | Integer | `4` | Maximum attempts per retryable read. Range 1 to 10. |
| `request_timeout_ms` | Integer | `30000` | Per-request budget covering the response body, pacing and all retries. Range 100 to 300,000. |
| `endpoint` | String | `https://api.ashbyhq.com` | Optional custom HTTPS origin with no path other than slash, and no query, fragment or credentials. Set trust_certificate with it. |
| `trust_certificate` | String | — | Worker-local PEM trust anchor for a custom origin. Hostname verification remains enabled. |

## Dates, sync and recovery

- `applications`, `application_feedback` and `interview_schedules` send `start_date` as Ashby's creation-time filter in milliseconds. Applications with a returned `updatedAt` are additionally filtered at or after that date. For application history, SeaTunnel discovers application parents without the creation-time filter and includes eligible parents by `updatedAt`. A missing update time is retained.
- A batch runs one provider pass. A streaming job starts another pass only after the final response is emitted and the poll interval passes. Application history uses the application list's sync token only after all history child pages are emitted. History in later passes depends on which parent applications Ashby reports as changed.
- Checkpoints store the active pass, cursor and parent positions. If a checkpoint lands within a page, the reader re-fetches that page and resumes after the saved row offset once the payload digest matches. A different payload or incompatible read scope fails the restore. Use a new snapshot when required and plan for deduplication with the downstream key and sink semantics.
- Ashby documents a 14-day lifetime for cursors and sync tokens. Expired or invalid state, an oversized delta, or contradictory continuation causes the job to fail with a diagnostic. Protect checkpoints as sensitive application data. Use the default API origin for regular traffic. Operators configuring a different origin are responsible for choosing a trusted endpoint for the API key. [Ashby pagination and incremental sync](https://developers.ashbyhq.com/docs/pagination-and-incremental-sync)

## Example

Read applications in a batch job. Use `job.mode = STREAMING` and a positive `poll_interval_ms` for continuous sync.

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}

source {
  Ashby {
    resource = "applications"
    token = ${ASHBY_TOKEN}
    start_date = "2025-01-01T00:00:00Z"
    plugin_output = "ashby_rows"
  }
}

sink {
  Console {
    plugin_input = "ashby_rows"
  }
}
```

<ChangeLog />
