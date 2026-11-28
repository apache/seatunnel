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

import ChangeLog from '../changelog/connector-amazonkinesis.md';

# Amazon Kinesis Data Streams Sink

`AmazonKinesis` sends append-only rows to an existing Data Streams stream. JSON serializes the
whole input row. Text encodes a string column as UTF-8. Binary sends a BYTES column unchanged.

## Options

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `region` | String | Yes | — | AWS region for the stream. |
| `stream_name` | String | One stream identifier | — | Stream name in the account of the credentials. |
| `stream_arn` | String | One stream identifier | — | ARN in the selected region. |
| `access_key` | String | With explicit credentials | — | AWS access key ID. |
| `secret_key` | String | With explicit credentials | — | AWS secret access key. |
| `token` | String | For explicit temporary credentials | — | AWS session token. |
| `format` | String | No | `json` | `json`, `text` or `binary`. |
| `data_field` | String | With `text` or `binary` | — | Input column that contains the payload. |
| `partition_key_field` | String | No | — | Name of a string column used as the key. |
| `partition_key` | String | No | Generated | Fixed key. Select it or a field. With neither set, each record gets a generated key. |
| `batch_size` | Integer | No | `100` | Maximum records per batch, from 1 to 500. |
| `batch_bytes` | Integer | No | `5242880` | Maximum total payload and key bytes per batch, up to 10 MiB. |
| `record_bytes` | Integer | No | `1048576` | Maximum record payload and key bytes. Use a larger value only when the existing stream supports it. |
| `partial_attempts` | Integer | No | `4` | Attempts for explicitly failed batch entries, from 1 to 8. |
| `preserve_order` | Boolean | No | `false` | Send one record at a time and chain provider sequence numbers for each configured key in this writer session. |
| `api_timeout_ms` | Integer | No | `10000` | SDK call and socket timeout, from 500 to 60,000 ms. |

Omitting the key pair uses the AWS SDK credentials available to the SeaTunnel worker. Explicit
temporary credentials require both keys and the session token. Batched mode uses
`kinesis:PutRecords`. Ordered mode needs `kinesis:PutRecord` and a fixed or field partition key.
Encrypted stream writes can require additional KMS access.

## Batching, ordering, and recovery

Checkpoint-enabled jobs flush on size limits, flush signals where the engine supports them, and
checkpoints. Jobs without checkpointing send records immediately. The sink validates the full
batch response and retries only retryable failed entries, retaining their original partition keys
and payloads. A call whose acceptance is unknown fails the job. Recovery may replay records
already accepted by AWS.

AWS assigns order within a shard. Batched retries can result in a different arrival order.
Ordered mode sends serially and chains sequences for the same key within a writer, with a cap of
8,192 distinct keys per writer. Each independent writer, restart, and resharding event can affect
the observed order. Both the record and batch size include the UTF-8 bytes of the partition key.

```hocon
env {
  job.mode = "BATCH"
  checkpoint.interval = 10000
}
source {
  FakeSource {
    schema {
      fields {
        account = "string"
        message = "string"
      }
    }
    rows = [{kind = INSERT, fields = ["account-1", "hello"]}]
  }
}
sink {
  AmazonKinesis {
    region = "us-east-1"
    stream_name = "messages"
    format = "text"
    data_field = "message"
    partition_key_field = "account"
  }
}
```

[PutRecords](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html) ·
[PutRecord](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html)

## Changelog

<ChangeLog />
