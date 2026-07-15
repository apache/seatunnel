import ChangeLog from '../changelog/connector-hugegraph.md';

# HugeGraph Source Connector

`Source: HugeGraph`

## Description

The HugeGraph source connector reads graph data from Apache HugeGraph through the HugeGraph REST API.

V1 supports bounded full-label scans with a single reader. It reads either one vertex label or one edge label, checkpoints at page boundaries, and follows HugeGraph page markers until the server returns `page = null`.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)

## Options

| Name               | Type    | Required | Default  | Description |
|--------------------|---------|----------|----------|-------------|
| `host`             | String  | Yes      | -        | HugeGraph server host. |
| `port`             | Integer | Yes      | -        | HugeGraph server port. |
| `protocol`         | String  | No       | `http`   | Server protocol: `http` or `https`. HTTPS uses the JVM trust store. |
| `graph_name`       | String  | Yes      | -        | HugeGraph graph name. |
| `label`            | String  | Yes      | -        | Vertex label or edge label to read. |
| `schema`           | Object  | Yes      | -        | Output property columns declared with `schema.fields`. Reserved graph columns are added by the connector. |
| `label_type`       | Enum    | No       | `VERTEX` | Label type. Supported values: `VERTEX`, `EDGE`. |
| `page_size`        | Integer | No       | `1000`   | Number of records per HugeGraph page. Must be in range `[100, 10000]`. |
| `time_zone`        | String  | No       | Worker JVM default | ZoneId used to convert HugeGraph DATE epoch values, for example `UTC` or `Asia/Shanghai`. Set it explicitly when workers may use different JVM time zones. |
| `graph_space`      | String  | No       | `DEFAULT` | The graph space the graph belongs to. |
| `username`         | String  | No       | -        | HugeGraph username. |
| `password`         | String  | No       | -        | HugeGraph password. |
| `max_retries`      | Integer | No       | `3`      | Retries after the initial attempt. Set to `0` to disable retries. |
| `retry_backoff_ms` | Integer | No       | `5000`   | Backoff time between retries in milliseconds. |

## Output Schema

Vertex output columns:

```text
~id, ~label, <schema.fields columns...>
```

Edge output columns:

```text
~id, ~label, ~source_id, ~source_label, ~target_id, ~target_label, <schema.fields columns...>
```

Columns prefixed with `~` are reserved columns added by the connector. HugeGraph property keys cannot start with `~`, so they do not conflict with user properties.

## Type Mapping

`schema.fields` must match the HugeGraph property key type. The connector validates this before reading.

| HugeGraph type | SeaTunnel type |
|----------------|----------------|
| `TEXT`         | `STRING`       |
| `INT`          | `INT`          |
| `LONG`         | `BIGINT`       |
| `FLOAT`        | `FLOAT`        |
| `DOUBLE`       | `DOUBLE`       |
| `BOOLEAN`      | `BOOLEAN`      |
| `DATE`         | `TIMESTAMP`    |
| `UUID`         | `STRING`       |
| `BLOB`         | `BYTES`        |

### Multi-valued (LIST / SET) properties

A HugeGraph property whose cardinality is `LIST` or `SET` is read as a SeaTunnel `ARRAY`. Declare it in `schema.fields` as `array<T>`, where `T` is the SeaTunnel type of the element (from the table above). For example, a `LIST<TEXT>` property named `tags` is declared as `tags = "array<string>"`.

Notes:

- `SET` elements have no guaranteed order on the server; use `LIST` when order matters.
- If a property has cardinality `LIST`/`SET` on the server but is declared as a scalar (or vice versa), the job fails at startup with a message telling you the correct declaration.
- `BLOB` elements inside a `LIST`/`SET` are not supported.

## Example

```hocon
source {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    label = "person"
    label_type = "VERTEX"
    page_size = 1000
    schema = {
      fields = {
        name = "string"
        age = "int"
      }
    }
  }
}
```

## Changelog

<ChangeLog />
