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

import ChangeLog from '../changelog/connector-http-brevo.md';

# Brevo

Install `connector-http-brevo` and configure one `Brevo` source block per resource, with source parallelism set to `1`. Use a Brevo workspace API key authorized to read that resource and any required parent inventory. Reads use HTTPS on `api.brevo.com`. [Brevo API reference](https://developers.brevo.com/reference/quickstart-reference)

## Resources

| `resource` | Brevo GET path | Records | Default and max page size |
| --- | --- | --- | --- |
| `contacts` | `/v3/contacts` | Contacts | `1000` |
| `contacts_attributes` | `/v3/contacts/attributes` | Contact attribute definitions | — |
| `contacts_folders_lists` | `/v3/contacts/folders/{folderId}/lists` | Lists for each contact folder | `50` |
| `contacts_folders` | `/v3/contacts/folders` | Contact folders | `50` |
| `contacts_segments` | `/v3/contacts/segments` | Contact segments | `50` |
| `contacts_lists_contacts` | `/v3/contacts/lists/{listId}/contacts` | Contacts for each discovered list | `500` |
| `contacts_lists` | `/v3/contacts/lists` | Contact lists | `50` |
| `senders` | `/v3/senders` | Email senders | — |
| `companies` | `/v3/companies` | CRM companies | `500` |
| `companies_attributes` | `/v3/crm/attributes/companies` | Company attribute definitions | — |
| `crm_pipeline_stages` | `/v3/crm/pipeline/details` | Stages in the default pipeline | — |
| `crm_pipeline_details_all` | `/v3/crm/pipeline/details/all` | All pipelines and their stages | — |
| `crm_attributes_deals` | `/v3/crm/attributes/deals` | Deal attribute definitions | — |
| `crm_deals` | `/v3/crm/deals` | CRM deals | `500` |
| `crm_tasktypes` | `/v3/crm/tasktypes` | CRM task types | — |
| `crm_tasks` | `/v3/crm/tasks` | CRM tasks | `50` |
| `crm_notes` | `/v3/crm/notes` | CRM notes | `50` |
| `domains` | `/v3/senders/domains` | Sender domains | — |
| `webhooks` | `/v3/webhooks` | Webhooks in the default transactional scope | — |
| `account` | `/v3/account` | One workspace account object | — |
| `organization_invited_users` | `/v3/organization/invited/users` | Invited users | — |
| `emailCampaigns` | `/v3/emailCampaigns` | Email campaigns | `100` |
| `smsCampaigns` | `/v3/smsCampaigns` | SMS campaigns | `1000` |
| `inbound_email_events` | `/v3/inbound/events` | Inbound email events | `500` |
| `transactional_email_events` | `/v3/smtp/statistics/events` | Transactional email events | `5000` |
| `processes` | `/v3/processes` | Background processes | `50` |
| `products` | `/v3/products` | Catalog products | `1000` |
| `categories` | `/v3/categories` | Catalog categories | `100` |
| `email_templates` | `/v3/smtp/templates` | Transactional email templates | `1000` |

Brevo was previously named Sendinblue. The resource name `campaigns` resolves to `emailCampaigns`, `templates` resolves to `email_templates`, and `contacts` keeps its name. All three use the current Brevo REST endpoint.

Brevo can initialize its default CRM task types on the first task-type listing in a workspace without them.

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `token` | string | Required | Workspace API key for the `api-key` header. |
| `resource` | string | Required | One export resource listed above. |
| `start_date` | string | None | Inclusive lower ISO-8601 timestamp with an offset, to millisecond precision. Required for the two email event reports. |
| `end_date` | string | None | Inclusive upper ISO-8601 timestamp. If only `start_date` is supplied, the upper bound freezes at the initial scan time. |
| `page_size` | int | Resource default | Number of rows to request from the selected paginated endpoint. Range 1 to its table maximum. Parent discovery keeps its own default. |
| `max_pages` | int | `2000` | Maximum requests in each numbered pagination sequence, including any final empty-page probe. Range 1–20000. |
| `connect_timeout_ms` | int | `10000` | TCP connect timeout in milliseconds. Range 1–120000. |
| `read_timeout_ms` | int | `30000` | Socket read timeout in milliseconds. Range 1–120000. |
| `request_deadline_ms` | int | `60000` | Total deadline for a request and its retries, in milliseconds. Range 1–300000. |
| `max_attempts` | int | `4` | Maximum attempts for a retryable response, including the first. Range 1–10. |
| `max_response_bytes` | int | `8388608` | Maximum response bytes before and after gzip decompression. Range 1024–33554432. |
| `tls_trust_certificate` | string | JVM trust store | Engine-local PEM or DER file containing one or more trusted X.509 certificates. The connector uses this trust store for its own connection and verifies the provider hostname. |

## Dates and pagination

The two email event streams read consecutive windows of 30 days. Inbound requests use millisecond timestamps. Transactional requests use inclusive UTC calendar dates and then apply the precise time bounds locally. Other dated resources support a bounded or full scan. Dates apply to contact updates, linked contact updates, company and deal modification attributes, CRM task/note `updatedAt`, campaign updates, email event time, and product/category updates. Date options are rejected for other resources.

Contact, company, deal, product, and category reads request the documented modification-time filter with a one-millisecond overlap at the inclusive lower boundary. Campaigns and CRM tasks/notes use local modification checks over the raw pages because their date-range parameters do not specify the requested modification semantics. The scan never stops a page because its records were filtered out. [Contacts](https://developers.brevo.com/reference/get-contacts) · [CRM tasks](https://developers.brevo.com/reference/get-all-tasks) · [CRM notes](https://developers.brevo.com/reference/get-all-notes)

Offsets advance by raw rows and company pages start at 1. Parent inventories are traversed separately from the selected date interval. The sender domain endpoint documents no request arguments. If it reports additional pages, the connector fails with an explicit error because the export is incomplete. [Sender domain request](https://developers.brevo.com/docs/domain-creation-and-management)

## Output and recovery

| String column | Value |
| --- | --- |
| `entity_id` | Provider record identifier when available. The transactional email message ID can occur on multiple events; all observations are retained. |
| `parent_id` | Folder ID for `contacts_folders_lists` or list ID for `contacts_lists_contacts`. Null for other resources. |
| `modified_at` | UTC ISO-8601 time for the resource's selected update/event field when available, otherwise null. |
| `data` | Complete provider record as JSON, preserving nested fields and numeric values. Child records carry their numeric `folderId` or `listId`. Companies and deals expose their nested modification value as `last_updated_at` when available. |

The reader snapshots the active time window, parent traversal, page and row offset. On restore, it verifies ordered active-page fingerprints before skipping rows already emitted. A changed active page fails explicitly. Later pages still reflect a mutable provider API, so the connector cannot promise a point-in-time snapshot.

The connector verifies TLS and sends the key only to the fixed Brevo host. It rejects redirects and HTTP 200 error objects, retries transient HTTP errors within the request deadline, and honors Brevo's HTTP 429 delay header. [Brevo rate-limit headers](https://developers.brevo.com/docs/limit-headers)

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  Brevo {
    token = ${BREVO_API_KEY}
    resource = "contacts_lists_contacts"
    start_date = "2024-01-01T00:00:00Z"
    end_date = "2024-01-31T23:59:59.999Z"
    page_size = 250
  }
}
sink {
  Console {}
}
```

<ChangeLog />
