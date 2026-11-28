import ChangeLog from '../changelog/connector-http-solr.md';

# Solr

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

Indexes documents in an existing Apache Solr core or SolrCloud collection over HTTP. The plugin is `connector-http-solr`. The connection needs read access to the core's Schema API unique-key endpoint and write access to the core's `/update` handler. [Solr update API](https://solr.apache.org/guide/solr/latest/indexing-guide/indexing-with-update-handlers.html)

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [full CDC support](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md) on Zeta when `sink.flush.interval` is enabled

## Sink Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `base_url` | String | Yes | | HTTPS origin, such as `https://solr.example:8983`. An optional trailing slash is allowed. Omit the core path, query, fragment and credentials. |
| `collection` | String | Yes | | Name of the pre-created Solr core or collection. The connector appends `/solr/<collection>`. |
| `username` | String | No | | Optional Basic authentication user. Set together with `password`. |
| `password` | String | No | | Optional Basic authentication password. Use secret or variable substitution in shared jobs. |
| `allow_insecure_http` | Boolean | No | false | Permits cleartext HTTP for an explicitly trusted, isolated endpoint. |
| `unique_key` | String | No | id | Target document ID field. It must match the core's configured unique key and map to a STRING or integral input column. |
| `field_mapping` | Map | No | `{}` | Input column name to target field name. Unmapped columns use their input names. |
| `enable_delete` | Boolean | No | false | Accepts `DELETE` rows and deletes only the supplied document ID. |
| `batch_size` | Integer | No | 100 | Maximum consecutive rows of one operation per HTTP request, range 1–500. A value of 1 sends each row independently. |
| `commit_mode` | Enum | No | HARD | `HARD`, `SOFT`, or `NONE`. Applies at checkpoint, final preparation or a Zeta timer flush. |
| `request_timeout_ms` | Integer | No | 30000 | Absolute deadline per HTTP request, range 1–300000. |
| common-options | | No | | [Sink common options](../common-options/sink-common-options.md). |

## Input and update semantics

`INSERT` and `UPDATE_AFTER` write a complete document. Solr replaces an existing document with the same unique key. Unset and null fields are omitted, so a replacement removes previously stored values for those fields. `UPDATE_BEFORE` fails validation. For an opt-in `DELETE` row, the connector reads only the key value and issues a delete-by-ID.

Strings, booleans, integral numbers, finite floating-point numbers and supported scalar arrays map to ordinary Solr fields. Array columns require a multi-valued target field and ignore null elements. A DECIMAL is sent as a plain decimal string. Configure a Solr string field when every decimal digit must be preserved. Dates and local timestamps are converted using UTC. Timezone-aware timestamps use their actual instant, with Solr date-field precision determined by the target schema. Times are strings and BYTES are base64 text. Missing required fields and incompatible Solr field types are rejected by Solr. Nested documents, arbitrary maps, Solr atomic update modifiers and delete-by-query are outside the supported input contract.

The connector verifies the target unique key before its first mutation. Each document is limited to 1 MiB and each request to 4 MiB. Index and delete commands preserve the incoming order within one writer. Use one writer if order matters, for example when several operations target the same key.

## Commit and recovery

`HARD` sends a hard commit and waits for a searcher. `SOFT` waits for a refreshed searcher without requesting a hard commit. `NONE` leaves commit and search visibility timing to Solr. A commit can make other pending core updates visible. Empty flushes send no requests. A Zeta job can set `env.sink.flush.interval` to flush sparse streams. [Solr commits and transaction logs](https://solr.apache.org/guide/solr/latest/configuration-guide/commits-transaction-logs.html)

A batch or commit acknowledgement must report HTTP 200 and a successful Solr status without partial errors. A rejected or ambiguous request fails the job. Solr may already have applied some operations, including in a batch that ends in an error. The connector performs no automatic retries or core-wide rollbacks. Check Solr before replaying a failed job. Engine recovery can reissue earlier changes. Solr writes are outside SeaTunnel's checkpoint transaction and may become visible before a checkpoint.

## Example

```hocon
env {
  job.mode = BATCH
  parallelism = 1
  job.retry.times = 0
}

source {
  FakeSource {
    schema = { fields { event_key = string, title = string } }
    rows = [{ kind = INSERT, fields = ["evt-1", "Inventory update"] }]
  }
}

sink {
  Solr {
    base_url = "https://solr.example:8983"
    collection = "events"
    field_mapping = { event_key = "id", title = "title_s" }
    commit_mode = HARD
  }
}
```

## Changelog

<ChangeLog />
