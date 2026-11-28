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

import ChangeLog from '../changelog/connector-http-cbs-statline.md';

# CBS StatLine

> CBS StatLine source connector

The `CbsStatLine` source reads the `Observations` entity of one public Statistics Netherlands (CBS) StatLine dataset through its [OData v4 API](https://www.cbs.nl/nl-nl/onze-diensten/open-data/statline-als-open-data/snelstartgids-odata-v4). A row represents one observation cell, not a pivoted dataset row. No credentials are required.

## Supported features

- Bounded batch read with source parallelism `1`.
- Server-driven pagination via `@odata.nextLink`, with a maximum of 10,000 pages per run. Repeated continuation URLs, redirects and continuation URLs outside the configured dataset endpoint are rejected.
- No incremental reads, deletes, consistent point-in-time snapshot or exactly-once replay. A rerun starts at the first page. To replace a dataset, choose replacement or staging-and-swap behavior in the downstream sink.
- Only observations are read. Measure and dimension dictionaries require a separate fetch.

## Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `dataset_id` | String | Yes | — | StatLine dataset identifier, as shown in the OData URL. |
| `max_rows` | Integer | No | `0` | Positive values cap the output and send the initial OData `$top` query. Zero reads available pages up to the 10,000-page safety limit. Exceeding that limit fails the job. |
| `base_url` | String | No | `https://datasets.cbs.nl` | Origin serving the same API. Use HTTPS outside controlled test networks. |

## Resource limits and recovery

Each consumed page is limited to 64 MiB after HTTP decompression. One JSON string or property-name token is limited to 1 MiB of encoded bytes, including quotes and escapes. Each observation is staged within 4 MiB and 200,000 JSON tokens before tree conversion; nesting is limited to 64 levels and number text to 1,000 characters. Response headers are limited to 64 entries and 8,192 characters per line; continuation URLs are limited to 8,192 characters before URI resolution. Proxy CONNECT refusal bodies are limited to 64 KiB before native tunnel buffering or cleanup consumption. These fixed limits fail explicitly instead of accepting arbitrarily large responses.

Connection and socket-inactivity timeouts remain 30 seconds. A page additionally has 120 seconds of cumulative active fetch/decode time. Time spent waiting for the engine, checkpoints or downstream row throttling is excluded from that budget. HTTP and parsing run outside the collector checkpoint monitor; each accepted row returns to the engine's checkpoint handoff. Cancellation aborts the owned request and suppresses late delivery.

Observations are streamed. A later invalid envelope may fail after earlier rows were accepted. A positive `max_rows` stops immediately after its accepted prefix, without validating or draining the unread suffix. Checkpoints and savepoints retain null source state: restoration deliberately starts the entire scan again, including previously emitted rows. They do not store a cursor, provide exactly-once output or create a remote snapshot.

## Output

| Field | Type | Description |
| --- | --- | --- |
| `dataset_id` | STRING | Configured CBS dataset identifier. |
| `id` | BIGINT | CBS observation ID. IDs are scoped to the dataset. |
| `measure` | STRING | CBS measure code. |
| `value` | STRING | Numeric value rendered without double rounding (may use exponent notation), or null. Convert to the destination type only after choosing its precision. |
| `string_value` | STRING | CBS text value, if present; trailing spaces are retained. |
| `value_attribute` | STRING | CBS value attribute, if present. |
| `observation` | STRING | Compact JSON containing the API observation including all dataset-specific dimensions. |

## Example

The following job reads two observations from the public dataset used in the CBS OData quickstart. Remove `max_rows` to read the whole dataset.

```hocon
env {
  job.mode = "BATCH"
  parallelism = 1
}

source {
  CbsStatLine {
    dataset_id = "83765NED"
    max_rows = 2
  }
}

sink {
  Console {}
}
```

HTTP errors, invalid responses and pagination loops fail the job rather than returning an apparently complete dataset. For a temporary API outage or rate limit, retry the batch later.

## Changelog

<ChangeLog />
