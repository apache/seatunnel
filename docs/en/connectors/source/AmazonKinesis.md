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

# Amazon Kinesis Data Streams Source

`AmazonKinesis` reads an existing Data Streams stream using the shared-throughput AWS API. It
emits raw records with shard and sequence metadata. Each source stanza uses one active reader,
which rotates among eligible shards. A streaming job discovers resharding changes and processes
each child after both retained parents finish. If checkpointing is enabled, the parents must also
be present as complete in a successful checkpoint before the child starts.

A batch job freezes the initial retained shard list. It drains closed shards and reads open shards
until an empty response reports zero lag. A bounded batch can fail at its catch-up deadline when
the producer keeps writing. Catch-up is evaluated per shard as the reader advances.

## Options

| Option | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `region` | String | Yes | — | AWS region for the stream. |
| `stream_name` | String | One stream identifier | — | Stream name in the account of the credentials. |
| `stream_arn` | String | One stream identifier | — | ARN in the selected region. |
| `access_key` | String | With explicit credentials | — | AWS access key ID. |
| `secret_key` | String | With explicit credentials | — | AWS secret access key. |
| `token` | String | For explicit temporary credentials | — | AWS session token. |
| `start_position` | String | No | `TRIM_HORIZON` | `TRIM_HORIZON`, `AT_TIMESTAMP`, or `LATEST`. |
| `start_timestamp` | String | With `AT_TIMESTAMP` | — | ISO-8601 instant. |
| `record_limit` | Integer | No | `500` | Requested records per call, from 1 to 10,000. |
| `shard_poll_ms` | Integer | No | `250` | Minimum interval between reads of a shard, from 200 to 60,000 ms. |
| `discovery_interval_ms` | Integer | No | `30000` | Streaming shard discovery interval, from 1,000 to 600,000 ms. |
| `batch_timeout_ms` | Integer | No | `600000` | Per-attempt catch-up deadline for a batch job, from 1,000 to 86,400,000 ms. |
| `api_timeout_ms` | Integer | No | `10000` | SDK call and socket timeout, from 500 to 60,000 ms. |

Omitting the explicit access key pair uses the AWS SDK credentials available to the SeaTunnel
worker, including environment credentials and AWS instance or container credentials. Supply
an access key pair and a session token together for explicit temporary credentials. Grant
`kinesis:DescribeStreamSummary`, `kinesis:ListShards`, `kinesis:GetShardIterator`, and
`kinesis:GetRecords` for the stream. Encrypted streams can also require KMS access.

## Output and recovery

| Column | Type | Meaning |
| --- | --- | --- |
| `stream_arn` | STRING | Resolved stream ARN. |
| `shard_id` | STRING | Provider shard identifier. |
| `sequence_number` | STRING | Exact provider sequence within the shard. |
| `partition_key` | STRING | Original partition key when supplied by AWS. |
| `arrival_timestamp` | STRING | Approximate arrival timestamp in UTC, when supplied. |
| `data` | BYTES | Exact raw Kinesis record bytes. |

`LATEST` anchors at the initial coordinator time and uses an AWS timestamp-based iterator.
The job preserves this anchor when a shard has yet to deliver its first record. A checkpoint
stores each shard's last collected sequence and reacquires an iterator after that sequence on
restore. Rows after the last checkpoint can replay. AWS retention, timestamp precision,
shared read quotas, and an absent or expired provider position determine what data is available.
Source restore uses SeaTunnel checkpoints and remains scoped to one job. Separate jobs read their
configured starting positions.

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 1000
}
source {
  AmazonKinesis {
    region = "us-east-1"
    stream_name = "events"
    start_position = "TRIM_HORIZON"
  }
}
sink {
  Console {}
}
```

[GetRecords](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html) ·
[GetShardIterator](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html)

## Changelog

<ChangeLog />
