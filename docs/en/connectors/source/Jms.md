import ChangeLog from '../changelog/connector-jms.md';

# Jms

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

> Generic JMS queue source connector

## Description

Reads messages from one queue of any message broker that has a JMS 1.1 (`javax.jms`) client, using that broker's own client library. Examples are ActiveMQ Artemis, ActiveMQ Classic, IBM MQ, TIBCO EMS and Solace; AMQP 1.0 brokers are reached through the Apache Qpid JMS client.

The providers tested with this connector are listed under [Install the JMS client](#install-the-jms-client). Other providers are expected to work through their `javax.jms` client, but are not tested.

This connector does not replace the [ActiveMQ](Activemq.md) or [RabbitMQ](Rabbitmq.md) connectors, which keep their provider-specific features.

## Support Those Engines

> Flink<br/>
> SeaTunnel Zeta<br/>

The source requires a streaming job with checkpointing enabled. Spark is not supported.

## Key Features

- [ ] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Install the JMS client

The connector ships only the JMS 1.1 API (`geronimo-jms_1.1_spec` 1.1.1, Apache License 2.0). Broker client libraries have different licenses and versions, so they are not bundled. Put the client jars of your broker in `${SEATUNNEL_HOME}/plugins/connector-jms/` on every node that runs SeaTunnel. Jars in this directory are loaded only for this connector.

The jars below are the runtime dependencies Maven resolves for each tested client.

**ActiveMQ Artemis, core protocol** (`org.apache.activemq:artemis-jms-client:2.19.1`). This is the set the end-to-end test installs in `plugins/connector-jms/`:

```
artemis-jms-client-2.19.1.jar
artemis-core-client-2.19.1.jar
artemis-commons-2.19.1.jar
artemis-selector-2.19.1.jar
jakarta.jms-api-2.0.3.jar
jakarta.json-api-1.1.6.jar
johnzon-core-0.9.5.jar
jboss-logging-3.4.2.Final.jar
jgroups-3.6.13.Final.jar
commons-beanutils-1.9.4.jar
commons-collections-3.2.2.jar
commons-logging-1.2.jar
netty-buffer-4.1.73.Final.jar
netty-codec-4.1.73.Final.jar
netty-codec-http-4.1.73.Final.jar
netty-codec-socks-4.1.73.Final.jar
netty-common-4.1.73.Final.jar
netty-handler-4.1.73.Final.jar
netty-handler-proxy-4.1.73.Final.jar
netty-resolver-4.1.73.Final.jar
netty-transport-4.1.73.Final.jar
netty-transport-classes-epoll-4.1.73.Final.jar
netty-transport-classes-kqueue-4.1.73.Final.jar
netty-transport-native-epoll-4.1.73.Final-linux-x86_64.jar
netty-transport-native-kqueue-4.1.73.Final-osx-x86_64.jar
netty-transport-native-unix-common-4.1.73.Final.jar
netty-tcnative-classes-2.0.46.Final.jar
```

**ActiveMQ Classic, OpenWire** (`org.apache.activemq:activemq-client:5.15.16`):

```
activemq-client-5.15.16.jar
hawtbuf-1.11.jar
geronimo-j2ee-management_1.1_spec-1.0.1.jar
geronimo-jms_1.1_spec-1.1.1.jar
slf4j-api-1.7.32.jar
```

**AMQP 1.0 through Apache Qpid JMS** (`org.apache.qpid:qpid-jms-client:0.61.0`), tested against ActiveMQ Artemis:

```
qpid-jms-client-0.61.0.jar
proton-j-0.33.10.jar
geronimo-jms_2.0_spec-1.0-alpha-2.jar
slf4j-api-1.7.32.jar
netty-buffer-4.1.72.Final.jar
netty-codec-4.1.72.Final.jar
netty-codec-http-4.1.72.Final.jar
netty-common-4.1.72.Final.jar
netty-handler-4.1.72.Final.jar
netty-resolver-4.1.72.Final.jar
netty-transport-4.1.72.Final.jar
netty-transport-classes-epoll-4.1.72.Final.jar
netty-transport-classes-kqueue-4.1.72.Final.jar
netty-transport-native-epoll-4.1.72.Final-linux-x86_64.jar
netty-transport-native-kqueue-4.1.72.Final-osx-x86_64.jar
netty-transport-native-unix-common-4.1.72.Final.jar
netty-tcnative-classes-2.0.46.Final.jar
```

The ActiveMQ Classic and Qpid JMS clients are tested with brokers embedded in the connector's unit tests; the ActiveMQ Artemis client is also tested in the end-to-end test against an `apache/activemq-artemis` container.

## Options

| Name | Type | Required | Default |
| --- | --- | --- | --- |
| connection_factory_class | string | yes, or `jndi_connection_factory_name` | - |
| connection_factory_properties | map | no | - |
| jndi_connection_factory_name | string | yes, or `connection_factory_class` | - |
| jndi_initial_context_factory | string | with `jndi_connection_factory_name` | - |
| jndi_provider_url | string | no | - |
| jndi_properties | map | no | - |
| username | string | no | - |
| password | string | no | - |
| destination | string | yes | - |
| message_selector | string | no | - |
| format | enum | no | JSON |
| field_delimiter | string | no | , |
| max_in_flight_messages | int | no | 1000 |
| schema | config | yes | - |

Set exactly one of `connection_factory_class` and `jndi_connection_factory_name`. The options of the other mode are rejected.

### connection_factory_class [string]

The provider's `javax.jms.ConnectionFactory` implementation, created with its public no-argument constructor. Examples: `org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory` (ActiveMQ Artemis), `org.apache.activemq.ActiveMQConnectionFactory` (ActiveMQ Classic), `org.apache.qpid.jms.JmsConnectionFactory` (Qpid JMS).

### connection_factory_properties [map]

Properties applied to the connection factory through its public setters, in alphabetical order of the keys. The key `brokerURL` calls `setBrokerURL`. The value is converted to the setter's parameter type: `String`, `int`, `long` or `boolean`, or their boxed forms. A dotted key calls getters for the leading parts and the setter for the last part: `prefetchPolicy.queuePrefetch` calls `getPrefetchPolicy().setQueuePrefetch(...)`.

An unknown key, or a value that cannot be converted, fails the reader when it creates the factory. The error names the key but not the value.

The broker address is a property of the factory, for example `brokerURL` for ActiveMQ Artemis and ActiveMQ Classic, and `remoteURI` for Qpid JMS.

### jndi_connection_factory_name / jndi_initial_context_factory / jndi_provider_url / jndi_properties

Look up the connection factory in JNDI instead. `jndi_initial_context_factory` is the `java.naming.factory.initial` class and is required; `jndi_provider_url` is `java.naming.provider.url`; `jndi_properties` adds further environment entries. The initial context factory class must be in `plugins/connector-jms/` as well.

### username / password [string]

When both are set, the connection is opened with `createConnection(username, password)`; otherwise with `createConnection()`. Keep credentials in these two options rather than in broker URLs or in the property maps: they are never logged, and [config encryption](../../introduction/configuration/config-encryption-decryption.md) protects top-level options only, not values inside a map.

### destination [string]

The queue name, passed to `Session.createQueue`. Its syntax is defined by the provider. Topics are not supported.

### message_selector [string]

A JMS message selector, applied by the broker. Messages that do not match are left on the queue.

### format / field_delimiter

The body of a `TextMessage`, or of a `BytesMessage` read as UTF-8 text, is converted with `schema`. `JSON` maps an object or an array of objects. `TEXT` splits the body with `field_delimiter` (default `,`).

Any other message type, an empty body, or a body that does not match the schema fails the reader, and the message is not committed. An `ObjectMessage` is never deserialized. Error messages carry the JMS message id but not the body.

### max_in_flight_messages [int]

The maximum number of messages one reader has received but not yet committed. At the limit, the reader stops receiving until a checkpoint completes. Two consequences:

- A reader reads at most this many messages per checkpoint interval. With the default of 1000 and a `checkpoint.interval` of 10 seconds, that is about 100 messages per second per reader. Size `checkpoint.interval`, this option and the parallelism for the expected message rate.
- Keep it below any broker limit on uncommitted messages in one transaction, such as the IBM MQ queue manager attribute `MaxUncommittedMsgs`. A receive beyond such a limit fails the reader.

### schema [config]

The row schema; see [Schema Feature](../../introduction/concepts/schema-feature.md). Message headers and properties are not exposed as columns.

### Common Options

See [Source Common Options](../common-options/source-common-options.md).

## Delivery and Recovery

- Each reader opens one connection and a transacted session per checkpoint epoch. A checkpoint seals the session of the current epoch; when the checkpoint completes, the sealed sessions up to it are committed. The broker keeps every message until the session that received it is committed.
- Delivery is **at least once**. After a failure, cancellation or restore, uncommitted sessions are rolled back and the broker redelivers their messages, so rows can be emitted twice. Use an idempotent sink where duplicates matter. Order is not guaranteed across a redelivery or with parallel readers.
- An aborted checkpoint commits nothing; the next completed checkpoint commits its messages too.
- If a commit fails, the reader fails and the job fails over; on Zeta it is restored from the last completed checkpoint as long as `job.retry.times` allows. If the broker did not apply the commit, its messages are redelivered; if it applied the commit and only the reply was lost, they are not.
- A lost connection reported to the connection's exception listener fails the reader.
- Parallel readers are competing consumers of the queue. The checkpoint state holds only logical consumer slots.
- An empty queue is not the end of input; the job runs until it is cancelled.

## Provider Notes

- **ActiveMQ Classic prefetch.** With the default `queuePrefetch` of 1000, a sealed consumer keeps the messages it prefetched until its session is committed, so they wait for the next checkpoint and can be read after later messages. Set a low prefetch, for example `prefetchPolicy.queuePrefetch = 0` in `connection_factory_properties`. ActiveMQ Artemis and Qpid JMS return prefetched messages when the consumer of a sealed session is closed.
- **IBM MQ.** Keep `max_in_flight_messages` below the queue manager's `MaxUncommittedMsgs`.

## Example

ActiveMQ Artemis:

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  Jms {
    connection_factory_class = "org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory"
    connection_factory_properties {
      brokerURL = "tcp://artemis-host:61616"
    }
    username = "seatunnel"
    password = "******"
    destination = "orders"
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

ActiveMQ Classic with a low prefetch:

```hocon
source {
  Jms {
    connection_factory_class = "org.apache.activemq.ActiveMQConnectionFactory"
    connection_factory_properties {
      brokerURL = "tcp://activemq-host:61616"
      prefetchPolicy.queuePrefetch = 0
    }
    destination = "orders"
    schema {
      fields {
        id = bigint
        name = string
      }
    }
  }
}
```

An AMQP 1.0 broker through Qpid JMS, with a selector and delimited text:

```hocon
source {
  Jms {
    connection_factory_class = "org.apache.qpid.jms.JmsConnectionFactory"
    connection_factory_properties {
      remoteURI = "amqp://amqp-host:5672"
    }
    destination = "orders"
    message_selector = "region = 'eu'"
    format = TEXT
    field_delimiter = "|"
    schema {
      fields {
        id = bigint
        name = string
      }
    }
  }
}
```

A connection factory from JNDI, here with the ActiveMQ Artemis JNDI provider:

```hocon
source {
  Jms {
    jndi_initial_context_factory = "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory"
    jndi_connection_factory_name = "SeaTunnelFactory"
    jndi_properties {
      connectionFactory.SeaTunnelFactory = "tcp://artemis-host:61616"
    }
    destination = "orders"
    schema {
      fields {
        id = bigint
        name = string
      }
    }
  }
}
```

## Changelog

<ChangeLog />
