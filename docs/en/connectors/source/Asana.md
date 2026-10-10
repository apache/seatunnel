# Http-Asana

> Asana source connector

## Description

Reads tasks from the Asana REST API (`GET /tasks`) for one project. Built on
`connector-http-base`. Batch mode only.

## Key Features

- [x] batch
- [ ] stream
- [ ] exactly-once   (at-least-once; dedupe on `gid`)
- [ ] column projection
- [ ] parallelism
- [ ] support user-defined split

## Options

| name                        | type   | required | default                       |
|-----------------------------|--------|----------|-------------------------------|
| api_key                     | string | yes      | -                             |
| project_gid                 | string | yes      | -                             |
| modified_since              | string | no       | -                             |
| base_url                    | string | no       | https://app.asana.com/api/1.0 |
| retry                       | int    | no       | 3                             |
| retry_backoff_multiplier_ms | int    | no       | 100                           |
| retry_backoff_max_ms        | int    | no       | 10000                         |

### api_key
Asana Personal Access Token (raw token, no `Bearer ` prefix). Needs `tasks:read`.

### project_gid
Project whose tasks are read.

### modified_since
Optional ISO 8601 timestamp, e.g. `2026-10-01T00:00:00Z`. Asana counts a task as
modified when its own properties or associations change, but not when only a
subtask changes. The connector keeps no state between runs.

### retry
Max retries on HTTP 429 and 5xx (and, in the base client, IOExceptions).
Backoff doubles from `retry_backoff_multiplier_ms` up to `retry_backoff_max_ms`.

## Output columns

All columns are STRING: `gid`, `name`, `completed`, `completed_at`, `created_at`,
`modified_at`, `due_on`, `assignee_gid`, `assignee_name`, `permalink_url`.
`completed_at`, `due_on` and the assignee columns can be null.

## Example

```hocon
source {
  Asana {
    api_key = "<PAT>"
    project_gid = "1200000000000000"
    modified_since = "2026-10-01T00:00:00Z"
  }
}
```
(Copy the `env`, result-table and `sink` lines from the Splunk example.)

## Notes

Delivery is at-least-once; a restarted job begins from the first page. Rate-limit
handling is bounded backoff only; `Retry-After` is not honored.