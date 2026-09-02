import ChangeLog from '../changelog/connector-hugegraph.md';

# HugeGraph Sink Connector

`Sink: HugeGraph`

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

The HugeGraph sink connector allows you to write data from SeaTunnel to Apache HugeGraph, a fast and scalable graph database.

This connector supports writing data as vertices or edges, providing flexible mapping from relational data models to graph structures. It is designed for high-performance data loading.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md)

The connector writes rows as either vertices or edges. It supports insert, update, and delete row kinds, and it flushes buffered records by `batch_size`, checkpoint, or close.

:::caution

New `mappings` configurations default to `schema_save_mode = CREATE_SCHEMA_WHEN_NOT_EXIST` and create missing HugeGraph PropertyKey/VertexLabel/EdgeLabel definitions before writing. Legacy `schema_config` jobs retain the previous `ERROR_WHEN_SCHEMA_NOT_EXIST` behavior unless this option is explicitly set.

:::

## Configuration Options

| Name                | Type    | Required | Default Value | Description                                                                    |
| ------------------- | ------- | -------- | ------------- |--------------------------------------------------------------------------------|
| `host`              | String  | Yes      | -             | The host of the HugeGraph server.                                              |
| `port`              | Integer | Yes      | -             | The port of the HugeGraph server.                                              |
| `protocol`          | String  | No       | `http`        | Server protocol: `http` or `https`. HTTPS uses the JVM trust store.            |
| `graph_name`        | String  | Yes      | -             | The name of the graph to write to.                                             |
| `graph_space`       | String  | No       | `DEFAULT`     | The graph space the graph belongs to. |
| `username`          | String  | No       | -             | The username for HugeGraph authentication.                                     |
| `password`          | String  | No       | -             | The password for HugeGraph authentication.                                     |
| `batch_size`        | Integer | No       | 500           | The number of records to buffer before writing to HugeGraph in a single batch. |
| `batch_interval_ms` | Integer | No       | 5000          | Retained for compatibility. To schedule timer flush on Zeta, configure `sink.flush.interval` in the job `env` block. |
| `batch_failure_fallback` | Boolean | No  | true          | When a batch insert fails, fall back to inserting the batch record by record so a single bad ("poison") record no longer fails the whole batch. Failed records are logged and skipped; the rest succeed. If every record fails (systemic error), it is surfaced. Set to `false` to fail the whole batch instead. |
| `max_insert_errors` | Integer | No       | 500           | Maximum number of records that may be skipped by the single-record fallback (`batch_failure_fallback=true`) before the task is failed. Bounds the otherwise unlimited silent skipping of poison records. Set to `-1` for unlimited. Only applies when `batch_failure_fallback` is enabled. |
| `failure_data_path` | String  | No       | -             | Optional local directory. When set, every record skipped by the single-record fallback is appended (mapped id, label, properties and the server error) to a per-subtask file (`hugegraph-sink-failures-subtask-N.log`) for offline investigation. In cluster mode the file is created on the worker node running the sink subtask. |
| `check_vertex`      | Boolean | No       | false         | Whether the server verifies that an edge's source/target vertices exist when writing edges. When `false`, edges whose endpoints were never loaded are written as orphan edges (or trigger server-side phantom vertex auto-creation). Enable to reject such edges. |
| `max_retries`       | Integer | No       | 3             | Retries after the initial attempt. Set to `0` to disable retries.               |
| `retry_backoff_ms`  | Integer | No       | 5000          | Base backoff between retries in ms. Grows exponentially per attempt (`retry_backoff_ms * 2^(attempt-1)`), capped at `retry_backoff_max_ms`. |
| `retry_backoff_max_ms` | Integer | No    | 30000         | Upper bound in ms for the exponential retry backoff.                           |

## Sink Options

| Name                       | Type    | Required | Default Value | Description |
|----------------------------|---------|----------|---------------|-------------|
| `mappings`                 | List    | Yes      | -             | Recommended mapping configuration. Each entry maps input rows to one HugeGraph vertex or edge label. |
| `schema_save_mode`         | Enum    | No       | `CREATE_SCHEMA_WHEN_NOT_EXIST` for `mappings`; `ERROR_WHEN_SCHEMA_NOT_EXIST` for legacy `schema_config` | Schema management mode. |
| `data_save_mode`           | Enum    | No       | `APPEND_DATA` | How pre-existing data is handled before writing. `APPEND_DATA` keeps existing data. `DROP_DATA` deletes, once at job start, only the data of the labels this job targets (edges then vertices), preserving their schema and any other labels' data; the drop is scoped per label (so one table's drop does not wipe another) and is not re-run on checkpoint restart. |
| `delete_vertex_with_edges` | Boolean | No       | `false` for `mappings`; `true` for legacy `schema_config` | When true, DELETE rows for vertices also delete associated edges. |
| `schema_config`            | Object  | No       | -             | Deprecated legacy mapping object. Use `mappings` instead. Either `mappings` or `schema_config` must be specified. |
| `selected_fields`          | List    | No       | -             | Deprecated. Still honored with legacy `schema_config`; use mapping `properties` for new jobs. |
| `ignored_fields`           | List    | No       | -             | Deprecated. Still honored with legacy `schema_config`; use mapping `properties` for new jobs. |

If both `mappings` and `schema_config` are configured, `mappings` wins and `schema_config` is ignored with a warning.

## Timer Flush

Timer flush is an engine-level feature supported only by Zeta. Configure `sink.flush.interval` in the job `env` block to write pending HugeGraph records even when `batch_size` has not been reached. Spark and Flink do not inject `FlushSignal` records and therefore do not trigger this scheduled flush.

```hocon
env {
  sink.flush.interval = 5000
}
```

HugeGraph timer flush reuses the connector's synchronized batch flush. Failures are propagated to the engine instead of being delayed in a connector-owned background thread.

### Mapping Configuration (`mappings`)

Each `mappings` entry defines how input rows are mapped to one HugeGraph vertex or edge label.

| Name               | Type               | Required   | Default Value | Description                                                                                              |
| ------------------ |--------------------| ---------- | ------------- |----------------------------------------------------------------------------------------------------------|
| `type`             | String             | Yes        | -             | The type of graph element to map to. Must be `VERTEX` or `EDGE`.                                         |
| `label`            | String             | Yes        | -             | The label of the vertex or edge in HugeGraph.                                                            |
| `properties`       | `List<String>`       | No         | -             | Source field names written as HugeGraph properties. If empty, all input fields are considered.           |
| `ttl`              | Long               | No         | -             | The time-to-live for the vertex or edge in seconds.                                                      |
| `ttlStartTime`     | String             | No         | -             | The start time for the TTL.                                                                              |
| `enableLabelIndex` | String             | No         | -             | Reserved label-index setting passed through the mapping config.                                          |
| `userdata`         | `Map<String, Object>` | No         | -             | User-defined data associated with the label.                                                             |
| `idStrategy`       | String             | For Vertex | -             | The ID generation strategy for vertices, such as `PRIMARY_KEY`, `CUSTOMIZE_STRING`, `CUSTOMIZE_NUMBER`, `CUSTOMIZE_UUID`, or `AUTOMATIC`. |
| `idFields`         | `List<string>`       | For Vertex | -             | A list of source field names used to generate the vertex ID. Required when `idStrategy` is not `AUTOMATIC`. |
| `sourceConfig`     | Object             | For Edge   | -             | An object defining the mapping for the edge's source vertex. See `Source/Target Config` below.           |
| `targetConfig`     | Object             | For Edge   | -             | An object defining the mapping for the edge's target vertex. See `Source/Target Config` below.           |
| `frequency`        | String             | For Edge   | -             | The frequency of the edge, e.g., `SINGLE`, `MULTIPLE`.                                                   |
| `sortKeys`         | `List<String>`       | For Edge   | -             | **Source field names** (as they appear in the input row, *before* `fieldMapping` is applied) whose values distinguish edges sharing the same source and target vertices. Required when `frequency = MULTIPLE`. Example: with `fieldMapping = {event_time: created_at}`, use `sortKeys = [event_time]`, not `[created_at]`. |
| `fieldMapping`     | `Map<String, String>` | No       | -             | A map where the key is the source field name and the value is the target property name in HugeGraph.      |
| `valueMapping`     | `Map<String, Map<Object, Object>>` | No       | -             | Per-field value transform. Outer key = source field name; inner map = `originalValue -> newValue`.                                                                |
| `ignored`          | `List<String>`      | No         | -             | Blacklist of source fields excluded from properties (implicit mode only). Mutually exclusive with `properties` (which acts as the selected whitelist). |
| `updateStrategies` | `Map<String, String>` | No         | -             | Per-property merge strategy on write, keyed by target property name: `OVERRIDE`, `APPEND`, `SUM`, `UNION`, `BIGGER`, `SMALLER`, etc. When set, existing elements are merged instead of overwritten. |
| `nullableKeys`     | `List<String>`        | No       | -             | Explicit allow-list of property keys that may be null on an auto-created label. When set, it overrides the default below (only these keys are nullable). Key properties (primary keys, `MULTIPLE`-edge sort keys) are always excluded. Mutually exclusive with `notNullableKeys`. |
| `notNullableKeys`  | `List<String>`        | No       | -             | Opt-out list used with the default nullability. By default, when neither `nullableKeys` nor `notNullableKeys` is set, all non-key properties of an auto-created label are nullable; list here the properties that must instead be required. Mutually exclusive with `nullableKeys`. Only affects newly auto-created labels. |
| `nullValues`       | `List<String>`        | No       | -             | A list of string values that should be treated as `null`.                                                |
| `dateFormat`       | String              | No         | `yyyy-MM-dd`  | The date format for parsing date strings.                                                                |
| `extraDateFormats` | `List<String>`      | No         | -             | Additional date patterns tried in order, after `dateFormat`, when parsing date strings — for sources that mix multiple date formats. |
| `listFormat`       | Object              | No         | -             | How a raw string cell is parsed into SET/LIST property elements: `startSymbol` (default `[`), `endSymbol` (default `]`), `elemDelimiter` (default `,`), and `ignoredElems`. |
| `unfold`           | Boolean             | No         | false         | (Vertex) Expand a list-valued CUSTOMIZE id cell into one vertex per element. INSERT/append-only. |
| `unfoldSource`     | Boolean             | No         | false         | (Edge) Expand a list-valued source-endpoint id cell into multiple edges (CUSTOMIZE endpoint). INSERT/append-only. |
| `unfoldTarget`     | Boolean             | No         | false         | (Edge) Expand a list-valued target-endpoint id cell into multiple edges (cartesian with source). INSERT/append-only. |
| `timeZone`         | String              | No         | Worker JVM default | The time zone for date parsing. When omitted, the worker JVM default time zone is used, matching the HugeGraph Source so a Source→Sink round-trip preserves absolute times. |

### Legacy Schema Configuration (`schema_config`)

`schema_config` defines how one input stream is mapped to a specific vertex or edge label in HugeGraph. It is deprecated; new jobs should use `mappings`.

| Name               | Type               | Required   | Default Value | Description                                                                                              |
| ------------------ |--------------------| ---------- | ------------- |----------------------------------------------------------------------------------------------------------|
| `type`             | String             | Yes        | -             | The type of graph element to map to. Must be `VERTEX` or `EDGE`.                                         |
| `label`            | String             | Yes        | -             | The label of the vertex or edge in HugeGraph.                                                            |
| `tablePath`        | String             | No         | -             | Reserved table path value carried in the schema config.                                                  |
| `properties`       | `List<String>`       | No         | -             | A list of source field names for the vertex or edge.                                                     |
| `ttl`              | Long               | No         | -             | The time-to-live for the vertex or edge in seconds.                                                      |
| `ttlStartTime`     | String             | No         | -             | The start time for the TTL.                                                                              |
| `enableLabelIndex` | String             | No         | -             | Reserved label-index setting passed through the schema config.                                           |
| `userdata`         | `Map<String, Object>` | No         | -             | User-defined data associated with the label.                                                             |
| `idStrategy`       | String             | For Vertex | -             | The ID generation strategy for vertices, such as `PRIMARY_KEY`, `CUSTOMIZE_STRING`, `CUSTOMIZE_NUMBER`, `CUSTOMIZE_UUID`, or `AUTOMATIC`. |
| `idFields`         | `List<string>`       | For Vertex | -             | A list of source field names used to generate the vertex ID.                                             |
| `sourceConfig`     | Object             | For Edge   | -             | An object defining the mapping for the edge's source vertex. See `Source/Target Config` below.           |
| `targetConfig`     | Object             | For Edge   | -             | An object defining the mapping for the edge's target vertex. See `Source/Target Config` below.           |
| `frequency`        | String             | For Edge   | -             | The frequency of the edge, e.g., `SINGLE`, `MULTIPLE`.                                                   |
| `mapping`          | Object             | No         | -             | An object defining advanced field and value mappings. See `Mapping Config` below.                        |

### Source/Target Config (`sourceConfig` and `targetConfig`)

This object is used within an `EDGE` schema to define how to identify the source and target vertices.

| Name       | Type         | Required | Default Value | Description                                                                                                                                                  |
| ---------- | ------------ | -------- | ------------- |--------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `label`    | String       | Yes      | -             | The label of the source or target vertex.                                                                                                                    |
| `idFields` | `List<String>` | Yes      | -             | A list of source field names from the input row used to construct the ID of the source/target vertex. The values will be concatenated to form the vertex ID. For a HugeGraph → HugeGraph clone, set this to the single reserved column that already carries the assembled endpoint id — `["~source_id"]` for `sourceConfig`, `["~target_id"]` for `targetConfig` — and the connector reuses that id directly (see *Cloning from a HugeGraph Source* below). |

### Cloning from a HugeGraph Source (reserved-id passthrough)

When the input is the HugeGraph Source, each row carries reserved columns holding the already-assembled element ids (`~id` for a vertex; `~source_id`/`~target_id` for an edge's endpoints). To clone losslessly:

- **Vertex** with a `CUSTOMIZE_STRING`/`CUSTOMIZE_NUMBER`/`CUSTOMIZE_UUID` id: set `idStrategy` to the matching `CUSTOMIZE_*` and `idFields = ["~id"]`; the original id is written verbatim. `PRIMARY_KEY` vertices instead reuse their key property columns (which the Source already emits), and `AUTOMATIC` ids cannot be preserved (the target server assigns new ones).
- **Edge**: set `sourceConfig.idFields = ["~source_id"]` and `targetConfig.idFields = ["~target_id"]`. The endpoint ids are reused directly, so edges clone regardless of the endpoint vertices' id strategies. The target endpoint vertex labels must already exist (the connector will not auto-create a vertex label from a reserved id).

### Mapping Config (`mapping`)

This object provides advanced control over how fields and values are mapped to properties.

| Name              | Type                | Required | Default Value | Description                                                                                                                                                                       |
| ----------------- |---------------------|----------| ------------- |-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `fieldMapping`    | `Map<String, String>` | No       | -             | A map where the key is the source field name and the value is the target property name in HugeGraph. If not specified, the source field name is used as the target property name. |
| `valueMapping`    | `Map<String, Map<Object, Object>>` | No       | -             | Per-field value transform. The outer key is the source field name; the inner map is `originalValue -> newValue`. Scoping by field prevents one column's rule from affecting another (e.g. `gender` M->male will not rewrite `status` M).                                            |
| `ignored`          | `List<String>`      | No         | -             | Blacklist of source fields excluded from properties (implicit mode only). Mutually exclusive with `properties` (which acts as the selected whitelist). |
| `updateStrategies` | `Map<String, String>` | No         | -             | Per-property merge strategy on write, keyed by target property name: `OVERRIDE`, `APPEND`, `SUM`, `UNION`, `BIGGER`, `SMALLER`, etc. When set, existing elements are merged instead of overwritten. |
| `nullableKeys`    | `List<String>`        | No       | -             | Explicit allow-list of property keys that may be null on an auto-created label. Overrides the nullable-by-default behavior. Mutually exclusive with `notNullableKeys`.             |
| `notNullableKeys` | `List<String>`        | No       | -             | Opt-out list of properties that must be required, used together with the nullable-by-default behavior. Mutually exclusive with `nullableKeys`.                                    |
| `nullValues`      | `List<String>`        | No       | -             | A list of string values that should be treated as `null`. Any field containing one of these values will not be written.                                                           |
| `dateFormat`      | String              | No       | `yyyy-MM-dd`  | The date format for parsing date strings.                                                                                                                                         |
| `extraDateFormats`| `List<String>`      | No       | -             | Additional date patterns tried in order, after `dateFormat`, when parsing date strings — for sources that mix multiple date formats.                                              |
| `listFormat`      | Object              | No       | -             | How a raw string cell is parsed into SET/LIST property elements: `startSymbol` (default `[`), `endSymbol` (default `]`), `elemDelimiter` (default `,`), and `ignoredElems`.        |
| `unfold`          | Boolean             | No       | false         | (Vertex) Expand a list-valued CUSTOMIZE id cell into one vertex per element. INSERT/append-only.                                                                                  |
| `unfoldSource`    | Boolean             | No       | false         | (Edge) Expand a list-valued source-endpoint id cell into multiple edges (CUSTOMIZE endpoint). INSERT/append-only.                                                                 |
| `unfoldTarget`    | Boolean             | No       | false         | (Edge) Expand a list-valued target-endpoint id cell into multiple edges (cartesian with source). INSERT/append-only.                                                             |
| `timeZone`        | String              | No       | Worker JVM default | The time zone for date parsing. When omitted, the worker JVM default time zone is used.                                                                                     |
| `sortKeys`         | `List<String>`        | For Edge   | -             | **Source field names** (before `fieldMapping` is applied) whose values distinguish edges sharing the same source and target vertices. Example: with `fieldMapping = {event_time: created_at}`, use `[event_time]`, not `[created_at]`.                                                                                                  |

## Supported Types

The connector validates the SeaTunnel row schema against the existing HugeGraph schema before writing.

| SeaTunnel type | HugeGraph property type |
|----------------|-------------------------|
| `BYTES`        | `BLOB`                  |
| `TINYINT`      | `INT`                   |
| `SMALLINT`     | `INT`                   |
| `INT`          | `INT`                   |
| `BIGINT`       | `LONG`                  |
| `FLOAT`        | `FLOAT`                 |
| `DOUBLE`       | `DOUBLE`                |
| `BOOLEAN`      | `BOOLEAN`               |
| `DATE`         | `DATE`                  |
| `TIMESTAMP`    | `DATE`                  |
| `ARRAY`        | A non-single-cardinality HugeGraph property whose element type is compatible |
| `STRING`       | `TEXT`                  |
| `DECIMAL`      | `TEXT`                  |
| `MAP`          | `TEXT`                  |
| `ROW`          | `TEXT`                  |
| `TIME`         | `TEXT`                  |
| `NULL`         | `TEXT`                  |

## Write Behavior Notes

- For vertices, `idStrategy` controls how the vertex ID is built. `PRIMARY_KEY` joins all `idFields` with HugeGraph's primary-key format, `CUSTOMIZE_STRING` joins multiple `idFields` with `:` (backslash-escaping any `:` in a value so distinct field tuples cannot collide; a single field is used verbatim), `CUSTOMIZE_NUMBER` expects one integer-valued field (a fractional value like `1.9` is rejected, not silently truncated), and `CUSTOMIZE_UUID` expects one UUID field.
- For edges, the connector reads the ID strategy from the existing source and target vertex labels in HugeGraph. `sourceConfig.idFields` and `targetConfig.idFields` must provide enough fields to rebuild those vertex IDs.
- `INSERT` writes a new vertex or edge, `UPDATE_AFTER` updates the existing graph element, and `DELETE` deletes it. Delete rows only need the fields required to build the element ID.
- `AUTOMATIC` vertex IDs support INSERT only. UPDATE and DELETE require a reconstructable ID strategy.
- The sink is at-least-once. Replayed INSERT records with `AUTOMATIC` IDs can create duplicates after retries or checkpoint recovery.
- Edge batches use the configured `check_vertex` (default `false`). With the default, vertices and edges may be written out of order and the graph reaches its final consistent state after all batches complete; set `check_vertex=true` to have the server reject edges whose endpoints do not yet exist.
- `nullValues` treats matching string values as null and skips those properties during writes.
- Time zone is configured **per mapping** via `timeZone` (there is no top-level `time_zone` option on the sink, unlike the HugeGraph Source, because date parsing is a per-mapping concern). When omitted it defaults to the worker JVM zone, matching the Source so a Source→Sink round-trip preserves absolute times.

## Usage Examples

The examples below use the default `schema_save_mode = CREATE_SCHEMA_WHEN_NOT_EXIST`. If you set `schema_save_mode = ERROR_WHEN_SCHEMA_NOT_EXIST`, create the corresponding HugeGraph schema before running the job.

### 1. Writing Vertices

This example shows how to read from a `FakeSource` and write `person` vertices to HugeGraph. The vertex ID is based on the `name` field.

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_input = "fake_source"
    schema = {
      fields = {
        name = "string"
        age = "int"
      }
    }
  }
}

sink {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    mappings = [
      {
        type = "VERTEX"
        label = "person"
        idStrategy = "PRIMARY_KEY"
        idFields = ["name"]
        properties = ["name", "age"]
      }
    ]
  }
}
```

### 2. Writing Edges

This example syncs a relationship table to `knows` edges in HugeGraph. The source table contains the names of the two people who know each other and the year they met.

```hocon
env {
  job.mode = "BATCH"
}

source {
  FakeSource {
    plugin_input = "fake_source"
    schema = {
      fields = {
        person1_name = "string"
        person2_name = "string"
        since = "int"
      }
    }
  }
}

sink {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    mappings = [
      {
        type = "EDGE"
        label = "knows"
        sourceConfig = {
          label = "person"
          idFields = ["person1_name"]
        }
        targetConfig = {
          label = "person"
          idFields = ["person2_name"]
        }
        properties = ["since"]
        fieldMapping = {
          person1_name = "name"
          person2_name = "name"
        }
      }
    ]
  }
}
```

### 3. Writing DELETE rows

The sink honours the row kind. `DELETE` rows only need to carry the columns
required to rebuild the element id; other columns can be omitted. With
`delete_vertex_with_edges = true`, deleting a vertex also deletes its incident
edges.

```hocon
source {
  FakeSource {
    schema = {
      fields = {
        name = "string"
      }
    }
    rows = [
      {
        kind = DELETE
        fields = ["bob"]
      }
    ]
  }
}

sink {
  HugeGraph {
    host = "localhost"
    port = 8080
    graph_name = "hugegraph"
    delete_vertex_with_edges = true
    mappings = [
      {
        type = "VERTEX"
        label = "person"
        idStrategy = "PRIMARY_KEY"
        idFields = ["name"]
      }
    ]
  }
}
```

### 4. Cloning from a HugeGraph Source

When the upstream is the HugeGraph Source, each row already carries the
reserved columns with the assembled element ids (`~id` for a vertex;
`~source_id` / `~target_id` for an edge). Reusing these ids preserves the
original graph exactly. The example below sets `multi_table_sink_replica` so the
sink can fan out across parallel writers when the source reads multiple labels.

```hocon
env {
  job.mode = "BATCH"
}

source {
  HugeGraph {
    host = "src-host"
    port = 8080
    graph_name = "hugegraph"
    label_type = "VERTEX"
  }
}

sink {
  HugeGraph {
    host = "dst-host"
    port = 8080
    graph_name = "hugegraph"
    multi_table_sink_replica = 2
    batch_size = 500
    mappings = [
      {
        type = "VERTEX"
        label = "person"
        idStrategy = "CUSTOMIZE_STRING"
        idFields = ["~id"]
      }
    ]
  }
}
```

## Changelog

<ChangeLog />
