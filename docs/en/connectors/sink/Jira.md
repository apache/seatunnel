import ChangeLog from '../changelog/connector-http-jira.md';

# Jira

{/*
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
*/}

> Jira Cloud sink connector

## Description

Create, edit, or delete Jira Cloud issues using REST API v3. Each sink entry selects one operation and sends each input row as an individual mutation. The Jira sink is included in the `connector-http-jira` plugin.

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Sink Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `base_url` | String | Yes | | Trusted Jira Cloud origin, such as `https://example.atlassian.net`. An optional port or trailing slash is allowed. An API path, user information, query, or fragment is invalid. |
| `api_version` | Integer | No | 3 | Jira REST API version. Only `3` is supported. |
| `username` | String | Yes | | Atlassian account email for HTTP Basic authentication. |
| `token` | String | Yes | | Atlassian API token for direct requests to the configured Jira Cloud site. Use secret or variable substitution in a shared configuration. |
| `operation` | Enum | Yes | | `CREATE`, `UPDATE`, or `DELETE`. |
| `allow_insecure_http` | Boolean | No | false | Enables cleartext HTTP for an explicitly trusted, isolated development endpoint. Keep the default when using Jira Cloud. |
| `request_timeout_ms` | Integer | No | 30000 | Total time budget for a row's connection, request, response, and retry delays. Range 1–300000. |
| `max_retries` | Integer | No | 2 | Maximum retries after Jira rejects an UPDATE with HTTP 429 and a valid `Retry-After` header. Range 0–10. |
| common-options | | No | | [Sink common options](../common-options/sink-common-options.md). |

## Input schema

Every row must have the `INSERT` kind. The selected operation determines the exact schema. Column order is arbitrary.

| Operation | Required STRING columns | Jira request |
| --- | --- | --- |
| `CREATE` | `fields` | `POST /rest/api/3/issue`. Include `project` and `issuetype` objects and a nonempty `summary` in `fields`. Jira can require additional fields for the selected project and issue type. |
| `UPDATE` | `issue_id_or_key`, `fields` | `PUT /rest/api/3/issue/{issueIdOrKey}`. Supply the issue ID or key and the fields to replace. |
| `DELETE` | `issue_id_or_key` | `DELETE /rest/api/3/issue/{issueIdOrKey}?deleteSubtasks=false`. Jira rejects deleting an issue that still has subtasks. |

`fields` is a nonempty JSON object. Use Jira field names and value shapes, such as `{"priority":{"id":"3"},"assignee":null}`. Explicit JSON nulls are sent to Jira, which decides whether the field can be cleared. Description, environment, and multi-line text custom fields use Atlassian Document Format. Requests with a JSON body are limited to 1 MiB and 64 levels of nesting in the input `fields` object. Duplicate JSON member names are rejected. See the [Jira issue API](https://developer.atlassian.com/cloud/jira/platform/rest/v3/api-group-issues/) for project permissions and field formats.

Use a direct-site API token for Basic authentication. See [Atlassian authentication](https://developer.atlassian.com/cloud/jira/platform/basic-auth-for-rest-apis/). REST v2, Data Center, gateway-scoped tokens, OAuth, bulk requests, and specialized operations such as workflow transitions and adding comments are outside the connector's supported API.

## Acknowledgement and replay

CREATE requires HTTP 201 with a valid created issue ID and key. UPDATE and DELETE require a bodyless HTTP 204. In-flight rows block the writer until acknowledgement or failure. A checkpoint records engine progress, while Jira mutations are immediately visible and cannot be rolled back by SeaTunnel.

Only UPDATE requests that receive HTTP 429 with a valid `Retry-After` are retried by the connector. A retry waits at least as long as Jira requested and runs only within the row's deadline. Timeouts, transport failures, 5xx responses, and invalid success acknowledgements fail the job. Jira may already have applied the mutation. Check Jira before restarting after such a failure. An engine retry or replay can send a previously applied command again, including a CREATE that can make a duplicate issue. Use one writer when the order of commands for an issue matters. In Zeta, set `job.retry.times = 0` if operations require manual reconciliation before replay.

## Example

```hocon
env {
  job.mode = BATCH
  parallelism = 1
  job.retry.times = 0
}

source {
  FakeSource {
    schema = { fields { fields = string } }
    rows = [
      {
        kind = INSERT
        fields = ["""{"project":{"key":"DEMO"},"issuetype":{"name":"Task"},"summary":"Check nightly import"}"""]
      }
    ]
  }
}

sink {
  Jira {
    base_url = "https://example.atlassian.net"
    username = "${jira_username}"
    token = "${jira_token}"
    operation = CREATE
  }
}
```

## Changelog

<ChangeLog />
