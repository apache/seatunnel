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
