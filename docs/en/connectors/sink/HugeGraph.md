import ChangeLog from '../changelog/connector-hugegraph.md';

# HugeGraph Sink Connector

`Sink: HugeGraph`

## Description

The HugeGraph sink connector allows you to write data from SeaTunnel to Apache HugeGraph, a fast and scalable graph database.

This connector supports writing data as vertices or edges, providing flexible mapping from relational data models to graph structures. It is designed for high-performance data loading.

## Key Features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md)

The connector writes rows as either vertices or edges. It supports insert, update, and delete row kinds, and it can flush buffered records by `batch_size` or `batch_interval_ms`.

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
| `batch_interval_ms` | Integer | No       | 5000          | The maximum time in milliseconds to wait before flushing a batch.              |
| `max_retries`       | Integer | No       | 3             | Retries after the initial attempt. Set to `0` to disable retries.               |
| `retry_backoff_ms`  | Integer | No       | 5000          | The backoff time between retries in milliseconds.                              |

## Sink Options

| Name                       | Type    | Required | Default Value | Description |
|----------------------------|---------|----------|---------------|-------------|
| `mappings`                 | List    | Yes      | -             | Recommended mapping configuration. Each entry maps input rows to one HugeGraph vertex or edge label. |
| `schema_save_mode`         | Enum    | No       | `CREATE_SCHEMA_WHEN_NOT_EXIST` for `mappings`; `ERROR_WHEN_SCHEMA_NOT_EXIST` for legacy `schema_config` | Schema management mode. |
| `delete_vertex_with_edges` | Boolean | No       | `false` for `mappings`; `true` for legacy `schema_config` | When true, DELETE rows for vertices also delete associated edges. |
| `schema_config`            | Object  | No       | -             | Deprecated legacy mapping object. Use `mappings` instead. Either `mappings` or `schema_config` must be specified. |
| `selected_fields`          | List    | No       | -             | Deprecated. Still honored with legacy `schema_config`; use mapping `properties` for new jobs. |
| `ignored_fields`           | List    | No       | -             | Deprecated. Still honored with legacy `schema_config`; use mapping `properties` for new jobs. |

If both `mappings` and `schema_config` are configured, `mappings` wins and `schema_config` is ignored with a warning.

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
| `valueMapping`     | `Map<Object, Object>` | No       | -             | A map to transform specific field values.                                                                |
| `nullableKeys`     | `List<String>`        | No       | -             | A list of property keys that can have null values.                                                        |
| `nullValues`       | `List<String>`        | No       | -             | A list of string values that should be treated as `null`.                                                |
| `dateFormat`       | String              | No         | `yyyy-MM-dd`  | The date format for parsing date strings.                                                                |
| `timeZone`         | String              | No         | `GMT+8`       | The time zone for date parsing.                                                                          |

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
| `idFields` | `List<String>` | Yes      | -             | A list of source field names from the input row used to construct the ID of the source/target vertex. The values will be concatenated to form the vertex ID. |

### Mapping Config (`mapping`)

This object provides advanced control over how fields and values are mapped to properties.

| Name              | Type                | Required | Default Value | Description                                                                                                                                                                       |
| ----------------- |---------------------|----------| ------------- |-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `fieldMapping`    | `Map<String, String>` | No       | -             | A map where the key is the source field name and the value is the target property name in HugeGraph. If not specified, the source field name is used as the target property name. |
| `valueMapping`    | `Map<Object, Object>` | No       | -             | A map to transform specific field values. The key is the original value from the source, and the value is the new value to be written.                                            |
| `nullableKeys`    | `List<String>`        | No       | -             | A list of property keys that can have null values.                                                                                                                                |
| `nullValues`      | `List<String>`        | No       | -             | A list of string values that should be treated as `null`. Any field containing one of these values will not be written.                                                           |
| `dateFormat`      | String              | No       | `yyyy-MM-dd`  | The date format for parsing date strings.                                                                                                                                         |
| `timeZone`        | String              | No       | `GMT+8`       | The time zone for date parsing.                                                                                                                                                   |
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

- For vertices, `idStrategy` controls how the vertex ID is built. `PRIMARY_KEY` joins all `idFields` with HugeGraph's primary-key format, `CUSTOMIZE_STRING` joins them with `:`, `CUSTOMIZE_NUMBER` expects one numeric field, and `CUSTOMIZE_UUID` expects one UUID field.
- For edges, the connector reads the ID strategy from the existing source and target vertex labels in HugeGraph. `sourceConfig.idFields` and `targetConfig.idFields` must provide enough fields to rebuild those vertex IDs.
- `INSERT` writes a new vertex or edge, `UPDATE_AFTER` updates the existing graph element, and `DELETE` deletes it. Delete rows only need the fields required to build the element ID.
- `AUTOMATIC` vertex IDs support INSERT only. UPDATE and DELETE require a reconstructable ID strategy.
- The sink is at-least-once. Replayed INSERT records with `AUTOMATIC` IDs can create duplicates after retries or checkpoint recovery.
- Edge batches use HugeGraph `check_vertex=false`, so vertices and edges may be written out of order. This is intentional; the graph reaches its final consistent state after all batches complete.
- `nullValues` treats matching string values as null and skips those properties during writes.

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

## Changelog

<ChangeLog />
