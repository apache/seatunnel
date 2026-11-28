import ChangeLog from '../changelog/connector-amazondatafirehose.md';

# Amazon Data Firehose

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

Writes records to an existing Amazon Data Firehose DirectPut delivery stream through `PutRecordBatch`. The connector uses the `connector-amazondatafirehose` artifact. Grant `firehose:PutRecordBatch` for the target delivery stream. Firehose manages delivery to the configured downstream destination. [AWS Firehose batch API](https://docs.aws.amazon.com/firehose/latest/APIReference/API_PutRecordBatch.html)

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md) on Zeta when `sink.flush.interval` is set

## Sink Options

| Name | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `delivery_stream` | String | Yes | | Existing DirectPut stream name. Use 1–64 letters, digits, periods, underscores or hyphens. |
| `region` | String | Yes | | AWS Region used to resolve and sign the Firehose request. |
| `access_key` | String | No | | Explicit AWS access key ID. Specify with `secret_key` or omit both to use worker environment credentials. |
| `secret_key` | String | No | | Explicit AWS secret access key. |
| `token` | String | No | | Optional AWS session token used with an explicit credential pair. |
| `endpoint_url` | String | No | | Trusted HTTPS origin override. The origin can have a port and an optional trailing slash. |
| `allow_insecure_http` | Boolean | No | false | Allows cleartext HTTP for an explicitly configured, trusted and isolated development endpoint. |
| `format` | Enum | No | JSON | `JSON`, `DELIMITED`, `JSON_LINES`, or `RAW`. |
| `field_delimiter` | String | No | `,` | Single character field separator for `DELIMITED`. Quotes and line breaks are invalid delimiters. |
| `max_record_bytes` | Integer | No | 1024000 | Maximum bytes for one record before base64 encoding, including the record separator. Range 1–1024000. |
| `batch_size` | Integer | No | 100 | Maximum number of records per request. Range 1–500. |
| `batch_timeout_ms` | Integer | No | 30000 | Total time for one flush including all attempts and backoff waits. Range 1–300000. |
| `max_retries` | Integer | No | 2 | Maximum attempts after the first one for explicitly failed records. Range 0–10. |
| common-options | | No | | [Sink common options](../common-options/sink-common-options.md). |

With an omitted credential pair, the worker reads `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and an optional `AWS_SESSION_TOKEN`. Provide credentials to the workers through your deployment's credential management. Direct SDK profile or metadata-provider resolution is outside this configuration.

## Input formats

Each input row must have kind `INSERT` and becomes one Firehose record.

| Format | Input | Output |
| --- | --- | --- |
| `JSON` | Structured SeaTunnel row | One UTF-8 JSON object plus an LF. |
| `DELIMITED` | Structured SeaTunnel row | One UTF-8 delimited record with CSV quoting plus an LF. Use a comma for CSV or a tab for TSV. Null fields are empty. |
| `JSON_LINES` | Exactly one STRING column named `payload` | One validated, single-line JSON object plus an LF. The connector preserves the original JSON representation, including decimal precision. |
| `RAW` | Exactly one STRING or BYTES column named `payload` | String as UTF-8 or supplied bytes exactly. Supply any separators required by the downstream reader. |

The connector splits batches at the count threshold or before the estimated request exceeds 4 MiB. It checks the actual request length as well. It flushes a pending batch at checkpoint/final preparation. Zeta can also flush below the threshold when `env.sink.flush.interval` is positive. Choose a checkpoint or flush interval for low-volume streaming. [AWS Firehose quotas](https://docs.aws.amazon.com/firehose/latest/dev/limits.html)

## Delivery and retries

A validated acknowledgement means Firehose accepted the records for asynchronous delivery. SeaTunnel retries only positions explicitly reported as failed by a complete, consistent batch response. Whole-request errors, invalid acknowledgements and lost responses fail the job without resending the whole batch. Firehose may have accepted some records before a failure.

Checkpoint recovery can resend records already accepted by Firehose. Accepted data cannot be rolled back. Handle duplicates downstream if your workflow requires deduplication, and use the Firehose delivery logs and metrics to verify delivery to the final destination. Batch processing and partial retry do not guarantee downstream order.

## Example

```hocon
env {
  job.mode = BATCH
  parallelism = 1
}

source {
  FakeSource {
    schema = {
      fields {
        event_id = int
        detail = string
      }
    }
    rows = [{ kind = INSERT, fields = [1, "inventory changed"] }]
  }
}

sink {
  AmazonDataFirehose {
    delivery_stream = "inventory-events"
    region = "us-east-1"
    format = JSON
  }
}
```

## Changelog

<ChangeLog />
