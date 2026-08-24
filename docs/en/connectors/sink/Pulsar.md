import ChangeLog from '../changelog/connector-pulsar.md';

# Pulsar

> Pulsar sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Description

The Pulsar sink writes SeaTunnel rows to Apache Pulsar topics. It can write to one configured topic, or route multi-table records by the table id carried in each row.

## Supported DataSource Info

| Datasource | Supported Versions |
|------------|--------------------|
| Pulsar     | Universal          |

## Sink Options

| Name                     | Type   | Required | Default             | Description                                                                                                      |
|--------------------------|--------|----------|---------------------|------------------------------------------------------------------------------------------------------------------|
| topic                    | String | No       | -                   | Target Pulsar topic. Required for normal single-table writes. Optional only when multi-table rows carry table ids. |
| client.service-url       | String | Yes      | -                   | Pulsar client service URL, for example `pulsar://localhost:6650`.                                                 |
| admin.service-url        | String | Yes      | -                   | Pulsar admin HTTP URL, for example `http://localhost:8080`.                                                       |
| auth.plugin-class        | String | No       | -                   | Pulsar authentication plugin class name.                                                                         |
| auth.params              | String | No       | -                   | Parameters for the authentication plugin. Configure it together with `auth.plugin-class`.                         |
| format                   | String | No       | json                | Data format. The default format is json. Optional text and avro format.                                                                        |
| field_delimiter          | String | No       | ,                   | Field delimiter used when `format = "text"`.                                                                     |
| semantics                | Enum   | No       | AT_LEAST_ONCE       | Write consistency. Valid values: `NON`, `AT_LEAST_ONCE`, `EXACTLY_ONCE`.                                         |
| transaction_timeout      | Int    | No       | 600                 | Pulsar transaction timeout in seconds. Used by `EXACTLY_ONCE`.                                                    |
| pulsar.config            | Map    | No       | -                   | Extra producer properties passed to the Pulsar producer client.                                                   |
| message.routing.mode     | Enum   | No       | RoundRobinPartition | Routing mode for partitioned topics. Valid values: `SinglePartition`, `RoundRobinPartition`.                     |
| partition_key_fields     | Array  | No       | -                   | Fields used to build the Pulsar message key.                                                                     |
| multi_table_sink_replica | Int    | No       | 1                   | Writer replica count for multi-table writes.                                                                     |
| common-options           | Config | No       | -                   | Sink plugin common parameters. See [Sink Common Options](../common-options/sink-common-options.md).              |

## Parameter Interpretation

### topic [String]

The default Pulsar topic used by the sink.

For single-table pipelines, configure `topic`. For multi-table pipelines, the sink uses `SeaTunnelRow.getTableId()` as the target topic when the row has a table id, and falls back to `topic` only when the row does not carry one.

If neither the row table id nor `topic` is available, the sink fails fast with a configuration error.

### client.service-url [String]

Pulsar client service URL. Use the Pulsar protocol, for example `pulsar://localhost:6650`.

### admin.service-url [String]

Pulsar admin HTTP URL.

For example, `http://my-broker.example.com:8080`, or `https://my-broker.example.com:8443` for TLS.

### auth.plugin-class [String]

Name of the authentication plugin.

### auth.params [String]

Parameters for the authentication plugin.

For example, `key1:val1,key2:val2`.

### format [String]

Data format. The default format is `json`. Optional text and avro format. You can also use `text`. When using `text`, configure `field_delimiter` if the default comma delimiter is not suitable.
When using avro format, the Avro schema is derived from the upstream row type; no sink-side `schema` option is required.

### field_delimiter [String]

Field delimiter used by the `text` format. The default value is `,`.

### semantics [Enum]

Write consistency semantics.

- `AT_LEAST_ONCE`: the default. Messages may be duplicated after job restart or retry.
- `EXACTLY_ONCE`: writes messages through Pulsar transactions. The Pulsar cluster must enable transaction support, and `transaction_timeout` should be greater than the checkpoint interval.
- `NON`: sends messages directly without checkpoint coordination. Data may be duplicated or lost after restart, retry, or network errors.

### transaction_timeout [Int]

Pulsar transaction timeout in seconds. The default value is `600`.

This option is only used when `semantics = "EXACTLY_ONCE"`. If a transaction is not committed before the timeout, Pulsar aborts it automatically.

### pulsar.config [Map]

Extra Pulsar producer properties. These properties are passed to the Pulsar producer client.

### message.routing.mode [Enum]

Routing mode for partitioned topics.

- `SinglePartition`: without a message key, one partition is selected and all messages are sent to it. With a key, Pulsar hashes the key and sends the message to the selected partition.
- `RoundRobinPartition`: without a message key, messages are sent across partitions in round-robin order. Pulsar applies round-robin routing at the batching-delay boundary, not per individual message.

### partition_key_fields [Array]

Fields used to build the Pulsar message key.

For example, if the upstream row has `name` and `age`, setting `partition_key_fields = ["name"]` builds a JSON key such as `{"name":"Jack"}`. The selected fields must exist in the upstream schema.

### multi_table_sink_replica [Int]

Replica count for multi-table sink writers. It applies to all target topics in the multi-table sink.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Task Example

### Write FakeSource To Pulsar

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    row.num = 10
    schema = {
      fields {
        c_string = string
        c_int = int
        c_bigint = bigint
        c_double = double
        c_timestamp = timestamp
      }
    }
  }
}

sink {
  Pulsar {
    topic = "topic_test"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    format = json
    pulsar.config = {
      sendTimeoutMs = 30000
    }
  }
}
```

### Write With Message Key

```hocon
sink {
  Pulsar {
    topic = "orders"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    partition_key_fields = ["order_id"]
    message.routing.mode = "SinglePartition"
    format = json
  }
}
```

### Exactly-Once Write

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

sink {
  Pulsar {
    topic = "orders"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    semantics = "EXACTLY_ONCE"
    transaction_timeout = 600
    format = json
  }
}
```

### Multi-Table Write

When upstream rows carry table ids, the sink can route each row to the topic with the same table id. In this mode, `topic` can be omitted.

```hocon
sink {
  Pulsar {
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    multi_table_sink_replica = 2
    format = json
  }
}
```

### Write Text Messages With a Custom Delimiter

Set `format = text` and configure `field_delimiter` to serialize each row into a delimited text payload. Useful when the downstream consumer expects a simple flat format.

```hocon
sink {
  Pulsar {
    topic = "text_events"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    format = text
    field_delimiter = "|"
  }
}
```

### Write Avro Messages

Set `format = avro`. The connector derives the Avro schema from the upstream row type, so no sink-side `schema` option is required.

```hocon
sink {
  Pulsar {
    topic = "test_avro_topic_fake_source"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    format = avro
  }
}
```

### Write With Pulsar Producer Properties

Pass extra producer properties via `pulsar.config`. This is forwarded to the Pulsar producer client and can be used for tuning (timeouts, batching, compression, etc.).

```hocon
sink {
  Pulsar {
    topic = "topic_test"
    client.service-url = "pulsar://localhost:6650"
    admin.service-url = "http://localhost:8080"
    format = json
    pulsar.config = {
      sendTimeoutMs = 30000
      batchingMaxMessages = 1000
    }
  }
}
```

## Changelog

<ChangeLog />
