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

import ChangeLog from '../changelog/connector-http-assemblyai.md';

# AssemblyAI

Install `connector-http-assemblyai` and use `AssemblyAI` in a batch job to read transcripts already created in your account. Use an API key for the US or EU region that holds your transcripts. [List transcripts](https://www.assemblyai.com/docs/pre-recorded-audio/api-reference/transcripts/list) · [Cloud endpoints](https://www.assemblyai.com/docs/pre-recorded-audio/select-the-region)

## Resources

| Resource | Data | Row ID |
| --- | --- | --- |
| `transcripts` | Original transcript summaries from the list endpoint, including their status and creation date | Transcript ID |
| `transcript_sentences` | Sentences of each completed transcript | Transcript ID plus a one-based sentence position |
| `paragraphs` | Paragraphs of each completed transcript | Transcript ID plus a one-based paragraph position |
| `redacted_audio` | JSON status and signed download URL for completed transcripts that have audio redaction enabled | Transcript ID |

The source follows the list's older-page cursor and processes the selected resources for each transcript. Transcript summaries can include queued, processing, failed and deleted entries. Children are fetched only for completed transcripts without a deletion marker. Redacted audio is subject to AssemblyAI's 24-hour availability. The source exports the URL as metadata. Use an appropriate destination for this short-lived URL and transcript content. [Sentence results](https://www.assemblyai.com/docs/pre-recorded-audio/api-reference/transcripts/get-sentences) · [Paragraph results](https://www.assemblyai.com/docs/pre-recorded-audio/api-reference/transcripts/get-paragraphs) · [Redacted audio](https://www.assemblyai.com/docs/pre-recorded-audio/api-reference/transcripts/get-redacted-audio)

## Options

| Option | Type | Default | Description |
| --- | --- | --- | --- |
| `token` | string | required | AssemblyAI API key |
| `region` | string | `US` | Selects the US or EU API endpoint |
| `api_origin` | string | region endpoint | Optional fixed HTTPS origin. US uses `https://api.assemblyai.com` and EU uses `https://api.eu.assemblyai.com`. |
| `tls_trust_certificate` | string | JVM trust | Optional connector-specific X.509 trust certificate in PEM or base64 DER. HTTPS hostname verification remains active. |
| `resources` | list of string | all four | Resources processed per transcript, in the configured order |
| `start_date` | string | batch time minus 30 days | Inclusive transcript creation timestamp, ISO 8601 with offset |
| `end_date` | string | batch time | Inclusive transcript creation timestamp, ISO 8601 with offset, capped at batch start |
| `page_size` | integer | 100 | Transcript page size from 1 to 200 |
| `max_pages` | integer | 1000 | Maximum number of list pages per batch |
| `request_timeout_ms` | integer | 30000 | Deadline for a complete HTTPS attempt |
| `operation_timeout_ms` | integer | 120000 | Deadline for a read and eligible retries |
| `min_request_interval_ms` | integer | 100 | Minimum interval between HTTPS attempts |
| `max_retries` | integer | 3 | Additional attempts for transient failures |
| `max_response_bytes` | integer | 4194304 | Independent transfer and decoded JSON size limit |

AssemblyAI's current list API documents a 30-day visibility period. Creation windows filter the provider's available results for all children and summaries. The source interprets a provider creation time without an offset as UTC.

## Output and recovery

The four string columns are `resource`, `id`, `transcript_id` and `record`. The JSON `record` preserves the corresponding summary, sentence, paragraph or redacted-audio metadata object. The source sends read-only requests to the selected API origin and never fetches a media URL. The engine must trust that origin's TLS certificate.

One active reader saves the frozen time window, older cursor, parent ID and accepted row positions. On an in-page or child restore, it confirms the saved content fingerprint before resuming. Provider data can change during a scan. A changed saved page fails the job so a position cannot refer to a different transcript. Run a fresh batch to scan the current provider results. Delivery guarantees depend on the sink.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  AssemblyAI {
    token = ${ASSEMBLYAI_TOKEN}
    region = "US"
    resources = ["transcripts", "transcript_sentences", "paragraphs"]
  }
}

sink {
  Console {}
}
```

<ChangeLog />
