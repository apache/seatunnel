import ChangeLog from '../changelog/connector-hugegraph.md';

# HugeGraph Source Connector

`Source: HugeGraph`

## Description

The HugeGraph source connector allows you to read vertices or edges from Apache HugeGraph into SeaTunnel.

This connector performs bounded full-label scans with server-side paging. Schema inference is supported: when no user-defined `schema` is provided, the connector inspects the HugeGraph `PropertyKey` definitions to build the row type.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

## Configuration Options

| Name                | Type    | Required | Default Value | Description                                                                    |
| ------------------- | ------- | -------- | ------------- |--------------------------------------------------------------------------------|
| `host`              | String  | Yes      | -             | The host of the HugeGraph server.                                              |
| `port`              | Integer | Yes      | -             | The port of the HugeGraph server.                                              |
| `graph_name`        | String  | Yes      | -             | The name of the graph to read from.                                            |
| `graph_space`       | String  | No       | -             | The graph space of the graph to be operated on.                                |
| `username`          | String  | No       | -             | The username for HugeGraph authentication.                                     |
| `password`          | String  | No       | -             | The password for HugeGraph authentication.                                     |
| `protocol`          | String  | No       | `http`        | The protocol to use for HugeGraph server connection (`http` or `https`).       |
| `label`             | String  | Yes      | -             | The vertex or edge label to read from.                                         |
| `type`              | String  | Yes      | -             | The type of graph element to read. Must be `VERTEX` or `EDGE`.                 |
| `properties`        | List    | No       | -             | A list of property names to read. If not specified, all properties are read.   |
| `page_size`         | Integer | No       | 500           | The number of records to fetch per page from HugeGraph. Must be greater than 0.|
| `limit`             | Integer | No       | -             | The maximum number of records to read. Must be greater than 0 if specified.    |
| `schema`            | Object  | No       | -             | User-defined schema. When absent, the connector infers schema from HugeGraph.  |

## Data Type Mapping

When the connector infers schema from HugeGraph `PropertyKey` definitions, the following mapping is applied:

| HugeGraph type | SeaTunnel type | Notes |
|----------------|----------------|-------|
| `BOOLEAN`      | `BOOLEAN`      |       |
| `INT`          | `INT`          |       |
| `LONG`         | `LONG`         |       |
| `FLOAT`        | `FLOAT`        |       |
| `DOUBLE`       | `DOUBLE`       |       |
| `DATE`         | `LOCAL_DATE`   | Returned as `LocalDate` in UTC. HugeGraph server may return DATE as space-separated strings; the connector handles this internally. |
| `UUID`         | `STRING`       |       |
| `TEXT`         | `STRING`       |       |
| `BLOB`         | `STRING`       |       |
| `LIST` / `SET` | `ARRAY<T>`     | Where `T` is the mapped base type. `SINGLE` cardinality properties map directly. |

## Schema Inference Rules

- Vertex rows always include `id` (`STRING`) and `label` (`STRING`) as the first two fields, followed by the label's properties in declaration order.
- Edge rows always include `id` (`STRING`), `label` (`STRING`), `source_id` (`STRING`), and `target_id` (`STRING`) as the first four fields, followed by the label's properties.
- All identifier fields (`id`, `source_id`, `target_id`) are normalized to `STRING` at runtime to match the inferred schema.

## Usage Examples

### 1. Reading Vertices with Inferred Schema

```hocon
env {
  job.mode = "BATCH"
}

source {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    label = "person"
    type = "VERTEX"
  }
}

sink {
  Console {}
}
```

### 2. Reading Edges with User-Defined Schema

```hocon
env {
  job.mode = "BATCH"
}

source {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    label = "knows"
    type = "EDGE"
    properties = ["since"]
    page_size = 1000
    limit = 10000
    schema = {
      fields = {
        id = "string"
        label = "string"
        source_id = "string"
        target_id = "string"
        since = "int"
      }
    }
  }
}

sink {
  Console {}
}
```

### 3. HTTPS Connection

```hocon
source {
  HugeGraph {
    host = "hugegraph.example.com"
    port = 8443
    protocol = "https"
    graph_name = "hugegraph"
    username = "admin"
    password = "secret"
    label = "person"
    type = "VERTEX"
  }
}
```

## Changelog

<ChangeLog />
