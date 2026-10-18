import ChangeLog from '../changelog/connector-datahub.md';

# DataHub

> DataHub sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

The DataHub sink writes SeaTunnel rows to Alibaba Cloud DataHub.

The connector supports single-table writes and multi-table writes. The `topic`
value is used literally: in multi-table jobs, records from all input tables are
written to the same configured topic.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Before You Start

Create the DataHub project and topic before running the SeaTunnel job. The
DataHub topic schema must contain fields with the same names as the upstream
SeaTunnel schema fields, because the sink writes values by field name.

## Sink Options

| name           | type   | required | default value | description |
|----------------|--------|----------|---------------|-------------|
| endpoint       | string | yes      | -             | DataHub service endpoint. |
| accessId       | string | yes      | -             | Alibaba Cloud access ID used to access DataHub. |
| accessKey      | string | yes      | -             | Alibaba Cloud access key used to access DataHub. |
| project        | string | yes      | -             | DataHub project name. |
| topic          | string | yes      | -             | DataHub topic name. Used literally in both single-table and multi-table jobs. |
| timeout        | int    | no       | 3000          | Maximum client connection timeout in milliseconds. |
| retryTimes     | int    | no       | 3             | Maximum retry count when writing a record fails. |
| common-options | config | no       | -             | Sink plugin common options. See [Sink Common Options](../common-options/sink-common-options.md). |

### endpoint [string]

The DataHub service endpoint. It usually starts with `http` or `https`.

### accessId [string]

The Alibaba Cloud access ID used to access DataHub.

### accessKey [string]

The Alibaba Cloud access key used to access DataHub.

### project [string]

The DataHub project name.

### topic [string]

The DataHub topic name. The value is used as-is; no placeholder substitution is
performed. In multi-table jobs, records from all input tables are written to
this single topic.

The SeaTunnel field names must match the DataHub topic fields, because the sink
writes fields by name according to the topic schema.

### timeout [int]

The maximum client connection timeout in milliseconds.

### retryTimes [int]

The maximum retry count when writing a record fails.

### common options

Sink plugin common parameters, please refer to
[Sink Common Options](../common-options/sink-common-options.md) for details.
For multi-table writes, `multi_table_sink_replica` can be used with the common
sink options.

## Task Example

### Write One Table to One Topic

A simple batch job that writes records from a fake source to a single DataHub topic.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"
    schema = {
      fields {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  DataHub {
    endpoint = "https://datahub.example.aliyuncs.com"
    accessId = "your-access-id"
    accessKey = "your-access-key"
    project = "demo_project"
    topic = "user_topic"
    timeout = 3000
    retryTimes = 3
  }
}
```

### Write Multiple Input Tables to One Topic

The sink writes to the single `topic` configured on the sink; there is no
per-table topic routing. In multi-table jobs, rows from every input table go to
that topic, so the topic schema must contain the fields of all upstream tables.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_output = "fake"

    tables_configs = [
      {
        row.num = 100
        schema = {
          table = "users"
          fields {
            name = "string"
            age = "int"
          }
        }
      },
      {
        row.num = 200
        schema = {
          table = "orders"
          fields {
            order_id = "int"
            amount = "decimal(10, 2)"
          }
        }
      }
    ]
  }
}

sink {
  DataHub {
    endpoint = "https://datahub.example.aliyuncs.com"
    accessId = "your-access-id"
    accessKey = "your-access-key"
    project = "demo_project"
    topic = "user_topic"
    timeout = 3000
    retryTimes = 3
  }
}
```

## FAQ

### Does DataHub sink support exactly-once delivery?

No. The connector performs best-effort writes with bounded retries (`retryTimes`, default `3`). Failures beyond the retry budget are surfaced to the job rather than silently swallowed, so enable checkpointing at the job level if at-least-once replay from upstream is acceptable.

### How does multi-table routing work?

There is no per-table routing: all rows are written to the single `topic`
configured on the sink. The connector does not automatically create the target
topic; create it in the DataHub project beforehand and ensure its schema field
names match the upstream SeaTunnel schema fields.

### Why is `topic` a required field even for single-table jobs?

DataHub writes are schema-bound: the sink serializes each row against the field names declared on the target topic. A missing or empty `topic` leaves the connector with nowhere to write, so it is validated at job start rather than guessed from the project.

## Changelog

<ChangeLog />

