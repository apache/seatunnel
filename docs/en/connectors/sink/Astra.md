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

import ChangeLog from '../changelog/connector-astra.md';

# Astra DB

The `Astra` sink writes vector documents through the [Astra DB Serverless Data API](https://docs.datastax.com/en/astra-db-serverless/api-reference/dataapiclient.html).
Install `connector-astra` with `bin/install-plugin.sh`. Use SeaTunnel Zeta in batch or streaming mode with
sink parallelism 1 and an Astra application token that can read collection metadata and perform the chosen write mode.
Creating a missing collection also requires creation permission in the existing keyspace.

## Write modes

| Mode | Effect |
| --- | --- |
| `append` | Inserts chunks for each incoming record. Equal primary-key values in separate inputs remain distinct. |
| `overwrite` | Clears the exact configured namespace and stream once before writing in that job. A job with no input may still clear this scope on a flush. |
| `append_dedup` | Deletes all earlier chunks in the same namespace, stream and composite key, then inserts the incoming chunks. Multiple updates to a key are applied in input order. |

The sink creates a cosine vector collection when missing and checks dimensions and metric when reusing a collection.
It clears only documents carrying its matching scope fields. It never drops or recreates a collection.
For an existing collection with selective indexing, keep `_st_scope` and, for deduplication, `_st_record_id` indexed.
Run one writer job for a scope when using overwrite or deduplication.

In `append_dedup`, `DELETE` and `UPDATE_BEFORE` remove only the matching record's chunks. `INSERT` and
`UPDATE_AFTER` write chunks. Other modes ignore delete and before-image rows. Use a fixed namespace and stream
for overwrite. Append and deduplication also support per-row routing through the corresponding field options.

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `endpoint` | string | — | Required Astra database HTTPS origin. |
| `token` | string | — | Required Astra application token. Supply it as a protected value. |
| `keyspace` | string | — | Required existing Astra DB keyspace. |
| `collection` | string | — | Required vector collection name. It can be created if absent. |
| `mode` | string | `append` | One of `append`, `overwrite`, and `append_dedup`. |
| `namespace` | string | Input database | Static namespace. Uses an empty namespace if the input has no database name. |
| `stream` | string | Input table | Static stream. Defaults to the input table name. |
| `namespace_field` | string | `""` | Optional literal string path supplying the namespace per row. Supported by append modes. |
| `stream_field` | string | `""` | Optional literal string path supplying the stream per row. Supported by append modes. |
| `primary_keys` | list | `[]` | Ordered non-null scalar field paths. Required for `append_dedup`. |
| `text_fields` | list | `[]` | Fields included in the embedding text. Empty selects all fields. |
| `metadata_fields` | list | `[]` | Fields stored inside `metadata`. Empty selects all fields. |
| `field_name_mappings` | map | `{}` | Exact selected path or field name to destination name for text and metadata. |
| `omit_raw_text` | boolean | `false` | Omit the generated chunk text field. Metadata selection is independent. |
| `splitter` | string | `separator` | `separator`, `markdown`, or `code`. |
| `separators` | list | `["\n\n", "\n", " ", ""]` | Ordered literal values for separator splitting. An empty final value allows character boundaries. |
| `keep_separator` | boolean | `false` | Keep a selected literal separator at the next chunk boundary. |
| `markdown_level` | int | `1` | Deepest Markdown heading used as a preferred boundary, 1–6. |
| `code_language` | string | `""` | Required for `code`. Use one of the languages listed below. |
| `chunk_size` | int | `400` | Maximum GPT-2 tokens for each chunk, 1–8191. |
| `chunk_overlap` | int | `0` | Maximum repeated GPT-2 tokens, less than `chunk_size`. |
| `tokenizer_data_file` | string | `""` | Optional local copy of the verified tokenizer rank file on each writer node. |
| `embedding_mode` | string | — | Required. Select one embedding mode from the next section. |
| `embedding_secret_key` | string | `""` | Separate credential for the selected remote provider. May be empty for a compatible service without authentication. |
| `embedding_url` | string | Mode-specific | HTTPS origin for OpenAI, Azure OpenAI, or Cohere. For a compatible service, supply its full HTTPS API base. |
| `embedding_model` | string | `text-embedding-ada-002` | Model name for `openai_compatible`. |
| `embedding_deployment` | string | `""` | Azure OpenAI deployment name. Required in that mode. |
| `embedding_field` | string | `""` | Literal path to a precomputed numeric vector. Required for `from_field`. |
| `embedding_dimensions` | int | Mode-specific | 1–4096 for `from_field` and `openai_compatible`. Other modes have fixed dimensions. |
| `max_row_bytes` | int | `262144` | Maximum serialized input and projected text bytes, 256–4194304. |
| `max_buffer_bytes` | int | `4194304` | Estimated expanded pending bytes, 1024–33554432. |
| `max_body_bytes` | int | `4194304` | JSON request and response bytes, 1024–8388608. Insert requests also have a 4 MiB connector cap. |
| `max_chunks_per_row` | int | `100` | Maximum generated chunks from one input row, 1–500. |
| `max_delete_requests` | int | `500` | Maximum continuation requests per filtered cleanup, 1–100000. |
| `connect_timeout_ms` | int | `10000` | TCP connection timeout, 1–300000. |
| `read_timeout_ms` | int | `15000` | TLS handshake and response inactivity timeout, 1–300000. |
| `request_timeout_ms` | int | `30000` | Deadline for an HTTPS operation including retries, 1–300000. |
| `flush_timeout_ms` | int | `120000` | Total network budget for one buffer flush, 1–600000. |
| `max_attempts` | int | `3` | Attempts for an eligible request including the first, 1–10. |

## Embeddings and text processing

| Mode | Dimensions | Configuration and request |
| --- | --- | --- |
| `from_field` | Configured | Reads `embedding_field` from the row and reuses it for every chunk. |
| `fake` | 1536 | Locally generated deterministic synthetic vectors for test data. |
| `openai` | 1536 | `text-embedding-ada-002`, bearer key, HTTPS endpoint `api.openai.com`. |
| `azure_openai` | 1536 | Deploy a 1536-dimensional model, set its Azure origin, deployment name and API key. Uses the Azure v1 embeddings API. |
| `cohere` | 1024 | `embed-english-v3.0` for search documents, bearer key, HTTPS endpoint `api.cohere.com`. Uses float vectors and errors when input is too long. |
| `openai_compatible` | Configured | Supply the HTTPS API base, model, dimensions and the provider key when required. The sink appends `/embeddings` to the base. |

Cohere retired its older light v2 model. The replacement has different vector semantics even at the
same dimension. Use a fresh collection or re-embed existing data when changing models.
[Cohere retirement notice](https://docs.cohere.com/changelog/2026-04-04-embed-v2-aya-8b-retirement)

Text selections are rendered as one named value per line. Use dotted paths for nested fields and
zero-based array positions. Wildcards such as `users.*.name` select multiple values in their input
order. Use `field_name_mappings` to rename a selected path. Every data row needs at least one text
selection and a finite, nonzero vector of the exact dimension. Metadata fields are written under a
dedicated `metadata` object, and a path with multiple matches retains its array. A dot in a flattened
metadata key is literal and must be escaped according to the Data API when filtering.

Sizes and overlap use true GPT-2 byte-pair tokens. The splitter prefers literal separators,
Markdown headings, or source-code boundaries before applying the token cap and keeps Unicode
characters intact. Code languages are `cpp`, `go`, `java`, `js`, `php`, `proto`, `python`, `rst`, `ruby`, `rust`, `scala`, `swift`, `markdown`, `latex`, `html`, `sol`.
The sink uses the JVM trust store for the pinned [public tokenizer rank file](https://openaipublic.blob.core.windows.net/encodings/r50k_base.tiktoken)
only when counting requires it. For offline workers set `tokenizer_data_file` to a copy with SHA-256
`306cd27f03c1a714eca7108e03d66b7dc042abe8c258b44c199a7ed9838dd930` on each worker. The download sends no credentials or row data.

## Output and recovery

Documents contain `_id`, `$vector`, selected `metadata`, optional `text`, and connector scope,
chunk index and payload acknowledgement fields. To exclude source text from Astra metadata,
choose `metadata_fields` that omit it as well as setting `omit_raw_text`. Remote modes send the
selected text to the embedding provider and can incur provider charges.

A flush sends at most 20 documents per insert. Larger scoped deletes continue in batches of
20 within the configured request and time limits. A successful checkpoint follows valid
acknowledgement of every buffered mutation and preserves the next sequence and overwrite fence.
The writes are visible before a checkpoint, and delete-plus-insert operations are not atomic.
A transport error or partial write may leave remote changes. The connector fails instead of
guessing the outcome. If a replay receives a duplicate ID, the stored payload digest must match
before the connector treats it as already acknowledged. Restoring a completed checkpoint
does not clear the overwrite scope a second time. Input ordering should be stable during replays.

## Example

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}
source {
  FakeSource {
    plugin_output = "articles"
    row.num = 1
    schema {
      fields {
        id = bigint
        body = string
        vector = "array<double>"
      }
    }
    rows = [{kind = "INSERT", fields = [42, "A document to index.", [0.5, 0.25, 1.0]]}]
  }
}
sink {
  Astra {
    plugin_input = "articles"
    endpoint = "https://example-us-east-2.apps.astra.datastax.com"
    token = ${ASTRA_DB_TOKEN}
    keyspace = "default_keyspace"
    collection = "sample_documents"
    mode = "append_dedup"
    namespace = "sample"
    stream = "articles"
    primary_keys = ["id"]
    text_fields = ["body"]
    metadata_fields = ["id"]
    embedding_mode = "from_field"
    embedding_field = "vector"
    embedding_dimensions = 3
  }
}
```

## Changelog

<ChangeLog />
