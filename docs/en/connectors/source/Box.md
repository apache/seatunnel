import ChangeLog from '../changelog/connector-http-box.md';

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

# Box

Install `connector-http-box` and use `Box` as the source name. It reads Box account resources and extracts text or Box AI results from existing files. The source uses one bounded split with parallelism 1. [Box API reference](https://developer.box.com/reference/)

## Resources

| Resource | Scope |
| --- | --- |
| `events`, `admin_logs` | User events and enterprise event history. Enterprise history requires administrative privileges. |
| `sign_templates`, `sign_request` | Visible Box Sign templates and requests. |
| `collections`, `collection_items` | Visible collections and their items. |
| `files`, `folders` | Direct file and folder children of the Box root. |
| `file_collaborations`, `file_comments`, `file_tasks` | Selected resources of each direct root file. |
| `folder_collaborations` | Collaborations of each direct root folder. |
| `recent_items`, `trashed_items`, `users` | Recently accessed items, trashed items and visible enterprise users. |
| `text_representation` | Box's extracted UTF-8 text from files in `folder_id`. |
| `ai_ask`, `ai_extract`, `ai_extract_structured` | A single-file AI answer, freeform extraction or structured extraction for files in `folder_id`. |

Selecting a child resource causes the source to discover its parents even when parent output is omitted. The extraction folder can be traversed recursively. Link destinations and collaboration or sharing URLs are preserved in the JSON payload and may contain sensitive data.

The output table `records` contains five STRING columns.

| Column | Description |
| --- | --- |
| `resource` | The selected Box resource. |
| `record_id` | The provider ID. Events use `event_id`, and recent items use the underlying item type and ID when provided. |
| `parent_id` | The containing folder, collection or file ID when applicable. |
| `payload` | The original catalog record or file detail as JSON. |
| `content` | Extracted text, a Box AI answer, or the complete structured AI response as JSON. Catalog rows have null content. |

## Configuration

| Option | Type | Default | Requirements and behavior |
| --- | --- | --- | --- |
| `resources` | LIST of STRING | | Required, with at least one distinct resource from the table above. |
| `auth_mode` | STRING | `bearer` | Use a supplied API token or `client_credentials`. |
| `token` | STRING | | Required for `bearer`. |
| `client_id` | STRING | | Required for `client_credentials`. |
| `secret_key` | STRING | | Required for `client_credentials`. Sent as `client_secret` in the grant form. |
| `box_subject_type` | STRING | | Required for `client_credentials`. Use `user` or `enterprise`. |
| `box_subject_id` | STRING | | Required for `client_credentials`. The decimal user or enterprise ID. |
| `endpoint` | STRING | `https://api.box.com` | Custom HTTPS API origin with no path, query, fragment or credentials. Requires `trust_certificate`. |
| `download_endpoint` | STRING | | Explicit custom HTTPS content origin. Requires a custom API origin and `trust_certificate`. |
| `trust_certificate` | STRING | | Worker-local PEM trust anchor for custom origins. Hostname validation remains enabled. |
| `page_size` | INT | 100 | Requested list limit, from 1 to 500. |
| `max_pages_per_parent` | INT | 1000 | Maximum pages per resource and parent, from 1 to 10000. |
| `max_requests` | INT | 50000 | Maximum logical API and content calls, from 1 to 1000000. Authentication and retries are bounded separately. |
| `max_attempts` | INT | 4 | Total attempts for an eligible HTTPS request, from 1 to 10. |
| `request_timeout_ms` | INT | 30000 | Per-acquisition time budget with retries and polling, from 100 to 300000 milliseconds. |
| `max_response_bytes` | INT | 8388608 | Decoded bytes allowed per successful response, from 1024 to 67108864. |
| `events_start_position` | STRING | `0` | Initial user event position. Use a decimal position or `now`. |
| `admin_start_position` | STRING | `0` | Initial enterprise history position. Use a decimal position or `now`. |
| `admin_created_after` | STRING | | Optional Box enterprise history lower bound in RFC 3339. |
| `admin_created_before` | STRING | | Optional Box enterprise history upper bound in RFC 3339. |
| `folder_id` | STRING | `0` | Root folder ID for extracted content. |
| `recursive` | BOOLEAN | false | Visit descendant folders for the selected extraction resources. |
| `max_depth` | INT | 32 | Maximum allowed extraction depth from the configured folder, from 0 to 64. |
| `max_items` | INT | 10000 | Maximum raw entries visited for extraction, from 1 to 100000. |
| `max_representation_polls` | INT | 8 | Maximum extracted-text status requests per file, from 1 to 100. |
| `ask_prompt` | STRING | | Required for `ai_ask`, up to 10000 characters. |
| `extract_prompt` | STRING | | Required for `ai_extract`, up to 10000 characters. |
| `structured_fields` | STRING | | Required for `ai_extract_structured`. JSON array of field definitions. |
| `file_fields` | LIST of STRING | | Optional file detail field selection. The source always includes `id`, `type` and `name` when a selection is supplied. |

Client credentials require a Box app authorized for the selected subject and resources. The source obtains and renews its own access token in memory. A supplied bearer token may expire independently of the scan. The Box account's permissions and entitlements control which files, administrative records and AI features are accessible. [Box app and token permissions](https://developer.box.com/guides/security)

## Pagination and file extraction

The source uses marker paging where Box supports it, and offset paging for collections, collection items and file comments. Offset advances by the limit returned by Box. A short or empty advanced page still follows an available continuation. When an offset-only route needs an offset beyond Box's 9999 limit, the source fails with that limit. Event reading uses `stream_position` and ends on an empty chunk. User event feeds can contain multiple occurrences of the same event ID. [Box pagination](https://developer.box.com/guides/api-calls/pagination/) [Box enterprise events](https://developer.box.com/guides/events/enterprise-events/for-enterprise)

Text extraction uses the `extracted_text` representation. Box generates it asynchronously for some files. Status reads stay on the API origin. Content can be retrieved from Box's documented content origins `dl.boxcloud.com` and `public.boxcloud.com` or an explicitly configured custom content origin. The source refuses redirects and verifies the hostname. With a custom API origin, configure `download_endpoint` to enable content requests. If Box lists no text representation, the file has empty text content. Status, permission and content errors fail the job. [Request a representation](https://developer.box.com/guides/representations/request-a-representation) [Download a representation](https://developer.box.com/guides/representations/download-a-representation)

File detail projection can include selectors such as `permissions` and `metadata.enterprise_12345.invoice`. Structured extraction defines a per-file schema using `key`, optional `type`, `description`, `displayName` or `display_name`, `prompt` and `options` with distinct `key` values for each field. Nested `fields` support structured and table definitions. Freeform extraction can also take a schema in `extract_prompt`. The API receives only existing file IDs and the configured prompt or field definitions. AI charges and file-format support depend on Box. [Structured AI extraction](https://developer.box.com/reference/post-ai-extract-structured)

## Recovery

Checkpoints preserve pending parent reads and the exact provider continuation. The source verifies a partly emitted page before publishing its remainder. Scope changes, repeated positions and recursive cycles fail explicitly. Concurrent changes can still affect Box lists during the scan.

A 429 can be retried within the limits when it includes a valid `Retry-After`. An ambiguous AI POST result fails the acquisition without an automatic retry. A later job restart can repeat an extraction whose output had not been checkpointed. Checkpoints never contain credentials, prompts or file text.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  Box {
    resources = ["files", "file_collaborations", "text_representation", "ai_extract_structured"]
    auth_mode = "client_credentials"
    client_id = ${BOX_CLIENT_ID}
    secret_key = ${BOX_CLIENT_SECRET}
    box_subject_type = "enterprise"
    box_subject_id = "12345"
    folder_id = "9001"
    recursive = true
    file_fields = ["permissions", "metadata.enterprise_12345.invoice"]
    structured_fields = """[{"key":"invoice_number","type":"string"}]"""
  }
}
```

## Changelog

<ChangeLog />
