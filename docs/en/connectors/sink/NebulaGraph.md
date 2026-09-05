import ChangeLog from '../changelog/connector-nebulagraph.md';

# NebulaGraph

> NebulaGraph sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

The NebulaGraph sink writes SeaTunnel rows as vertices under one existing tag. This first connector scope supports NebulaGraph 3.5 or later because it uses parameterized DML, which was introduced in NebulaGraph 3.5.

The target space and tag must exist before the job starts. Source reads, edge writes, schema creation, and delete handling are not included in this version.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)

## Options

| name | type | required | default value | description |
|------|------|----------|---------------|-------------|
| hosts | list | yes | - | NebulaGraph graphd addresses in `host:port` form. Bracketed IPv6 addresses are also supported. |
| username | string | yes | - | NebulaGraph username. |
| password | string | yes | - | NebulaGraph password. |
| space | string | yes | - | Existing NebulaGraph space. |
| tag | string | yes | - | Existing vertex tag. |
| vid_field | string | yes | - | Input field used as the vertex ID. |
| write_fields | list | no | all fields except `vid_field` | Input fields written as tag properties. |
| write_mode | enum | no | `INSERT` | `INSERT` or `UPDATE`. |
| batch_size | int | no | 500 | Number of vertices in each nGQL request. |
| timeout_millis | int | no | 30000 | Connection, socket, and session wait timeout in milliseconds. |
| max_retries | int | no | 0 | Retries after the initial write attempt. |
| retry_interval_millis | int | no | 1000 | Delay between retries in milliseconds. |
| common-options | | no | - | Sink common options. |

### write_mode [enum]

- `INSERT` accepts only `INSERT` rows and sends `INSERT VERTEX IF NOT EXISTS`. A replay does not overwrite an existing vertex.
- `UPDATE` accepts `INSERT` and `UPDATE_AFTER` rows, ignores `UPDATE_BEFORE`, and sends `UPDATE VERTEX`. The vertex must already exist.

`DELETE` rows are rejected in both modes.

### common options

Sink plugin common parameters, see [Sink Common Options](../common-options/sink-common-options.md) for details.

## Supported Types

The vertex ID may be a `STRING`, `TINYINT`, `SMALLINT`, `INT`, or `BIGINT`. It must not be null.

| SeaTunnel property type | NebulaGraph parameter value |
|-------------------------|-----------------------------|
| STRING | string |
| BOOLEAN | boolean |
| BYTES | binary |
| TINYINT / SMALLINT / INT / BIGINT | integer |
| FLOAT / DOUBLE | floating point |
| DATE | date |
| TIME | time |
| TIMESTAMP | datetime |

Other property types are rejected during sink initialization.

## Write Guarantees and Limits

- The sink provides at-least-once delivery. It flushes on `batch_size`, checkpoint preparation, and writer close.
- `max_retries` defaults to `0` because retrying a request after an ambiguous network failure can repeat a write. Enable retries only when the selected write mode is safe for the job.
- Each sink block writes vertices to one tag. Use separate sink blocks for different tags.
- Space, tag, and property names must use letters, digits, or underscores and must not start with a digit.
- The connector uses the default NebulaGraph Thrift socket transport. TLS and HTTP/2 transport options are not exposed in this version.

## Task Example

Create the space and tag before running the job, for example:

```ngql
CREATE SPACE IF NOT EXISTS examples(vid_type = FIXED_STRING(64));
USE examples;
CREATE TAG IF NOT EXISTS person(name string, age int);
```

After the schema is available on graphd, the following job writes two vertices:

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 2
    schema = {
      fields {
        id = string
        name = string
        age = int
      }
    }
  }
}

sink {
  NebulaGraph {
    hosts = ["localhost:9669"]
    username = "root"
    password = "nebula"
    space = "examples"
    tag = "person"
    vid_field = "id"
    write_fields = ["name", "age"]
    write_mode = "INSERT"
  }
}
```

## Changelog

<ChangeLog />
