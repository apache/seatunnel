import ChangeLog from '../changelog/connector-activemq.md';

# ActiveMQ

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

> ActiveMQ Classic queue source connector

## Description

Reads JMS `TextMessage` payloads from one ActiveMQ Classic queue over OpenWire. Reuses the existing ActiveMQ module and JSON/text deserializers. This is not a generic JMS or AMQP connector.

## Support Those Engines

> Flink<br/>
> SeaTunnel Zeta<br/>

The source requires streaming mode and periodic checkpoints. Spark is not supported by this first slice: its virtual source checkpoints have not been validated as an end-to-end commit boundary for destructive queue acknowledgements.

## Key Features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

| Name | Type | Required | Default |
| --- | --- | --- | --- |
| uri | string | yes | - |
| queue_name | string | yes | - |
| username | string | no | - |
| password | string | no | - |
| format | enum | no | JSON |
| field_delimiter | string | no | , |
| max_in_flight_messages | int | no | 1000 |
| schema | config | yes | - |

### uri [string]

One broker endpoint: `tcp://host:port` or `ssl://host:port`. Credentials, URI query options, composite transports (including `failover:`), and URI paths are not supported. Use the separate credential options. For TLS, configure the JVM trust/key stores on every worker and use `ssl://`; never disable certificate validation. Plain TCP should only be used on a trusted network.

The connector fails the task on a broken connection so the engine restores the reader with a new connection. It does not reconnect in place or reuse acknowledgement handles from an earlier session.

### queue_name [string]

One literal queue name. Wildcards, composite queues, destination prefixes and `?consumer.*` destination options are rejected. Topics, durable topic subscriptions and selectors are outside this first slice. Provision the queue and the broker account's permissions before starting the job; broker auto-creation policy is not controlled by the connector.

### username / password [string]

Optional credentials, which must be configured together. When omitted, the client connects without explicit credentials. Use a broker account scoped to the intended queue. Do not embed credentials in `uri`.

### format / field_delimiter

`JSON` maps an object or an array of objects using `schema`. `TEXT` uses the existing delimited text deserializer, with `field_delimiter` (default `,`). Only JMS `TextMessage` is accepted; `ObjectMessage`, `BytesMessage`, null or empty bodies, and invalid payloads fail the task without acknowledging that message. No Java object payload is deserialized. Deserialization/emission failures omit the original exception details because those details can contain the queue payload; verify the schema and format against the message through an authorized broker client.

### max_in_flight_messages [int]

Positive maximum count of emitted, unacknowledged messages per reader. The source pauses receiving when this limit is reached and resumes after checkpoint completion. The ActiveMQ client queue prefetch is set to the same count; client-side prefetched messages may consume additional memory. This is not a byte limit. Size the limit and producer message sizes for available worker memory. Frequent successful checkpoints are needed for sustained throughput.

### schema [config]

Payload schema; see [Schema Feature](../../introduction/concepts/schema-feature.md). Message headers/properties are not exposed as metadata in this slice.

### Common Options

See [Source Common Options](../common-options/source-common-options.md).

## Delivery and Recovery

- Delivery is **at least once**, not exactly once. Each message is individually acknowledged only after a checkpoint containing its emitted rows completes. A later completed checkpoint can include messages from earlier aborted checkpoints.
- Failure before acknowledgement closes the connection and leaves messages eligible for broker redelivery. Duplicates are possible after recovery, including partially emitted multi-row JSON messages; use an idempotent downstream sink where needed.
- Checkpoint state contains logical consumer slots, not broker message objects or queue offsets. ActiveMQ remains responsible for retaining and redelivering unacknowledged messages.
- Each active parallel reader owns a connection, session and competing queue consumer. Normal startup assigns one slot per reader. Recovery preserves existing logical slots and adds slots when parallelism increases; a reader can own multiple restored slots while using one consumer after scale-down. No global ordering is guaranteed with parallel consumers or redelivery.
- An empty queue is not end-of-input. The source continues waiting until the job is cancelled or fails.
- Retention depends on broker persistence, producer delivery mode, expiration and dead-letter/redelivery policies. A durable broker and persistent messages are required to survive broker restarts. A permanently invalid message can repeatedly fail the job or be routed to a dead-letter queue by broker policy; this connector never silently skips it.

## Example

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  ActiveMQ {
    uri = "tcp://activemq-host:61616"
    queue_name = "events"
    format = JSON
    max_in_flight_messages = 1000
    schema {
      fields {
        id = bigint
        name = string
      }
    }
  }
}

sink {
  Console {}
}
```

For delimited messages such as `42|created`, set `format = TEXT` and `field_delimiter = "|"`, with schema fields in the payload's field order. Existing ActiveMQ **sink** configuration and behavior are unchanged; source options do not apply to the sink.

## Changelog

<ChangeLog />
