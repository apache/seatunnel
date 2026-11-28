import ChangeLog from '../changelog/connector-http-confluence.md';

# Confluence

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

## Description

The Confluence source reads one resource from the [Confluence Cloud REST API](https://developer.atlassian.com/cloud/confluence/rest/v2/intro/) in each bounded batch run. Use separate source blocks to read multiple resources.

| Resource | API | Records |
| --- | --- | --- |
| `pages` | REST v2 | Pages visible to the account, including the selected body representation |
| `blog_posts` | REST v2 | Blog posts visible to the account, including the selected body representation |
| `space` | REST v2 | Visible spaces, with plain description and icon information where available |
| `group` | REST v1 | Groups visible to an account with Confluence site access |
| `audit` | REST v1 | Retained audit events accessible to an account with the Confluence Administrator global permission |

## Install and authenticate

Install the `connector-http-confluence` plugin using the SeaTunnel plugin installer for your SeaTunnel version. The source uses standard TLS certificate and hostname verification. Create a classic Atlassian API token and use its account email as `username`. Store the token through your deployment's secret mechanism. [Atlassian Basic authentication](https://developer.atlassian.com/cloud/confluence/basic-auth-for-rest-apis/)

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| site_url | String | Yes | - | Trusted HTTPS site origin, such as `https://example.atlassian.net`. Use the origin without `/wiki`. |
| username | String | Yes | - | Atlassian account login email. |
| token | String | Yes | - | Classic Atlassian API token for the account. |
| resource | String | Yes | - | One of `pages`, `blog_posts`, `space`, `group`, or `audit`. |
| body_format | String | No | `storage` | Page and blog-post body representation. Use `storage` or `atlas_doc_format`. |
| page_size | Int | No | `25` | Requested records per page, 1–100. The provider can return fewer. |
| max_pages | Int | No | `10000` | Maximum pages per run, 1–50000. A continuation beyond this limit fails the run. |
| max_attempts | Int | No | `4` | Total attempts per page, 1–10, including the initial request. |
| connect_timeout_ms | Int | No | `10000` | Connection timeout, 1–300000 milliseconds. |
| read_timeout_ms | Int | No | `30000` | Socket inactivity timeout, 1–300000 milliseconds. |
| page_timeout_ms | Int | No | `120000` | Total page deadline including retries and waits, 1–600000 milliseconds. |
| max_response_bytes | Int | No | `16777216` | Maximum decompressed response bytes per page, 1–33554432. |

The source follows validated cursor or offset continuations. It keeps credentials on the configured origin and rejects redirects. Eligible transient failures retry the same page. A provider `Retry-After` value is a minimum. If the remaining deadline is shorter, the page fails.

## Output and recovery

| Column | Type | Meaning |
| --- | --- | --- |
| resource | String | Name of the selected API resource. |
| id | String | Provider record ID when present. |
| audit_created_at | Bigint | Audit `creationDate` as supplied by the API. Null for other resources and where absent. Audit records may share a timestamp. |
| content | String | JSON object returned for the record, including nested fields and nulls. |

Use `job.mode = "BATCH"` and `parallelism = 1` for each source. A new job starts a full refresh. Zeta checkpoint and savepoint restore resumes the saved page position and validates any already emitted prefix on that page. A changed prefix or expired cursor can require a new full refresh. Rows emitted after the restored checkpoint can replay with a nontransactional sink. Concurrent changes to the remote collection can affect the pagination results. The source does not provide a point-in-time remote snapshot.

The source supports Confluence Cloud with classic API tokens. Scoped or service-account tokens use a separate Atlassian gateway and are unsupported. Data Center, comments, attachments, membership expansion, writes, and incremental or change-data capture are outside this source's scope.

## Examples

Read pages to Console.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Confluence {
    site_url = "https://example.atlassian.net"
    username = "reader@example.com"
    token = "replace-with-classic-api-token"
    resource = "pages"
  }
}
sink {
  Console {}
}
```

Read pages and spaces as separate tables.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Confluence {
    site_url = "https://example.atlassian.net"
    username = "reader@example.com"
    token = "replace-with-classic-api-token"
    resource = "pages"
    result_table_name = "confluence_pages"
  }
  Confluence {
    site_url = "https://example.atlassian.net"
    username = "reader@example.com"
    token = "replace-with-classic-api-token"
    resource = "space"
    result_table_name = "confluence_spaces"
  }
}
sink {
  Console {
    source_table_name = "confluence_pages"
  }
  Console {
    source_table_name = "confluence_spaces"
  }
}
```

<ChangeLog />
