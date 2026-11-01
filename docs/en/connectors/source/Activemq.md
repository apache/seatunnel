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
| message_selector | string | no | - |
| receive_timeout | int | no | 200 |
| client_id | string | no | - |
| close_timeout | int | no | 5000 |
| connect_response_timeout | int | no | 10000 |
| always_session_async | boolean | no | - |
| check_for_duplicate | boolean | no | - |
| dispatch_async | boolean | no | - |
| warn_about_unstarted_connection_timeout | int | no | - |
| consumer_expiry_check_enabled | boolean | no | - |
| schema | config | yes | - |

### uri [string]

One broker endpoint: `tcp://host:port` or `ssl://host:port`. Credentials, URI query options, composite transports (including `failover:`), and URI paths are not supported. Use the separate credential options. For TLS, configure the JVM trust/key stores on every worker and use `ssl://`; never disable certificate validation. Plain TCP should only be used on a trusted network.

The connector fails the task on a broken connection so the engine restores the reader with a new connection. It does not reconnect in place or reuse acknowledgement handles from an earlier session.

### queue_name [string]

One literal queue name. Wildcards, composite queues, destination prefixes and `?consumer.*` destination options are rejected. Topics and durable topic subscriptions are outside this first slice. Provision the queue and the broker account's permissions before starting the job; broker auto-creation policy is not controlled by the connector.

### username / password [string]

Optional credentials, which must be configured together. When omitted, the client connects without explicit credentials. Use a broker account scoped to the intended queue. Do not embed credentials in `uri`.

### format / field_delimiter

`JSON` maps an object or an array of objects using `schema`. `TEXT` uses the existing delimited text deserializer, with `field_delimiter` (default `,`). Only JMS `TextMessage` is accepted; `ObjectMessage`, `BytesMessage`, null or empty bodies, and invalid payloads fail the task without acknowledging that message. No Java object payload is deserialized. Deserialization/emission failures omit the original exception details because those details can contain the queue payload; verify the schema and format against the message through an authorized broker client.

### max_in_flight_messages [int]

Positive maximum count of emitted, unacknowledged messages per reader. The source pauses receiving when this limit is reached and resumes after checkpoint completion. The ActiveMQ client queue prefetch is set to the same count; client-side prefetched messages may consume additional memory. This is not a byte limit. Size the limit and producer message sizes for available worker memory. Frequent successful checkpoints are needed for sustained throughput.

### message_selector [string]

Optional JMS selector expression for broker-side filtering. Only messages matching the selector are delivered to the consumer. The selector syntax follows the JMS 1.1 specification, e.g. `JMSType = 'car'` or `priority > 5`. When omitted, all messages on the queue are delivered.

### receive_timeout [int]

Timeout in milliseconds for `consumer.receive()` on the first poll of each batch. Smaller values improve checkpoint responsiveness but increase CPU usage when the queue is idle. This timeout holds the reader's monitor — the same lock that guards `snapshotState` and the checkpoint complete/abort callbacks — so values close to the checkpoint interval may delay checkpoint barrier handling and completion. Maximum allowed value is 30000. Defaults to 200.

### client_id [string]

Sets the JMS clientID for the connection. Some broker authentication and audit policies require a non-empty clientID; it is also required for durable subscriptions (not applicable to queue sources). The source appends the reader's subtask index to the configured value (e.g., `my-client` becomes `my-client-0` for the first reader, `my-client-1` for the second) so parallel readers do not collide. When omitted, each reader uses a broker-generated connection ID. Because the JMS clientID is fixed for a connection's lifetime, a restart may fail with `javax.jms.InvalidClientIDException` if the broker has not yet released the previous connection's ID. If that happens, either omit `client_id` (let the broker generate one) or wait for the broker's connection-timeout before restarting.

### close_timeout [int]

Timeout in milliseconds before a close is considered complete. Normally `Connection.close()` waits for broker confirmation. This prevents the client from hanging when no broker is available. Defaults to 5000.

### connect_response_timeout [int]

Timeout in milliseconds for the connection establishment response from the broker. Defaults to 10000.

### always_session_async [boolean]

When true, a separate thread dispatches messages for each session. A separate thread is always used when there is more than one session, or the session is not in `AUTO_ACKNOWLEDGE` or `DUPS_OK_ACKNOWLEDGE` mode. Defaults to the ActiveMQ client default (true) when not set.

### check_for_duplicate [boolean]

When true, the consumer checks for duplicate messages and handles them to prevent double processing. Defaults to the ActiveMQ client default (true) when not set.

### dispatch_async [boolean]

Whether the broker dispatches messages asynchronously to the consumer. Defaults to the ActiveMQ client default (true) when not set.

### warn_about_unstarted_connection_timeout [int]

Timeout in milliseconds from connection creation to when a warning is generated if the connection is not started via `Connection.start()` and a message is received. Set to a negative value to disable the warning. Defaults to the ActiveMQ client default (500) when not set.

### consumer_expiry_check_enabled [boolean]

Controls whether message expiration checking is performed by each `MessageConsumer` before dispatching. Defaults to the ActiveMQ client default (true) when not set.

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
