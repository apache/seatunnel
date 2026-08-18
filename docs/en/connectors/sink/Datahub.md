import ChangeLog from '../changelog/connector-datahub.md';

# DataHub

> DataHub sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

The DataHub sink writes SeaTunnel rows to Alibaba Cloud DataHub.

The connector supports single-table writes and multi-table writes. In multi-table
jobs, use placeholders such as `${table}` in `topic` to route records from
different input tables to different DataHub topics.

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
| topic          | string | yes      | -             | DataHub topic name. Supports placeholders in multi-table jobs. |
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

The DataHub topic name. For multi-table writes, this value can contain
placeholders such as `${table}`. `${table_name}` is only kept as a
deprecated compatibility alias; use `${table}` for new jobs.

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

### Write Multiple Input Tables to Matching Topics

When the upstream source provides multiple tables, configure `topic` with the
`${table}` placeholder so each input table is routed to a topic with the same name.

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
    topic = "${table}"
    timeout = 3000
    retryTimes = 3
  }
}
```

## Changelog

<ChangeLog />
