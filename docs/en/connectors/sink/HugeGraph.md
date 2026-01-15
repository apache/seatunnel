import ChangeLog from '../changelog/connector-hugegraph.md';

# HugeGraph Sink Connector

`Sink: HugeGraph`

## Description

The HugeGraph sink connector allows you to write data from SeaTunnel to Apache HugeGraph, a fast and scalable graph database.

This connector supports writing data as vertices or edges, providing flexible mapping from relational data models to graph structures. It is designed for high-performance data loading.

## Features

- **Batch Writing**: Data is written in batches for high throughput.
- **Flexible Mapping**: Supports flexible mapping of source fields to vertex/edge properties.
- **Vertex and Edge Writing**: Can write data as either vertices or edges.
- **Automatic Schema Creation**: Can automatically create graph schema elements (property keys, vertex labels, edge labels) if they do not exist.

## Configuration Options

| Name                | Type    | Required | Default Value | Description                                                                    |
| ------------------- | ------- | -------- | ------------- |--------------------------------------------------------------------------------|
| `host`              | String  | Yes      | -             | The host of the HugeGraph server.                                              |
| `port`              | Integer | Yes      | -             | The port of the HugeGraph server.                                              |
| `graph_name`        | String  | Yes      | -             | The name of the graph to write to.                                             |
| `graph_space`       | String  | Yes      | -             | The graph space of the graph to be operated on.                                |
| `username`          | String  | No       | -             | The username for HugeGraph authentication.                                     |
| `password`          | String  | No       | -             | The password for HugeGraph authentication.                                     |
| `batch_size`        | Integer | No       | 500           | The number of records to buffer before writing to HugeGraph in a single batch. |
| `batch_interval_ms` | Integer | No       | 5000          | The maximum time in milliseconds to wait before flushing a batch.              |
| `max_retries`       | Integer | No       | 3             | The maximum number of times to retry a failed write operation.                 |
| `retry_backoff_ms`  | Integer | No       | 5000          | The backoff time between retries in milliseconds.                              |

## Sink Options

| Name               | Type   | Required | Default Value | Description                                                                                         |
| ------------------ | ------ | -------- | ------------- |-----------------------------------------------------------------------------------------------------|
| `schema_config`    | Object | Yes      | -             | The configuration for mapping the input data to HugeGraph's schema (vertices or edges).             |
| `selected_fields`  | List   | No       | -             | A list of fields to be selected from the input data. If not specified, all fields will be used.     |
| `ignored_fields`   | List   | No       | -             | A list of fields to be ignored from the input data. Mutually exclusive with `selected_fields`.      |

### Schema Configuration (`schema_config`)

Each object in the `schema_config` list defines a mapping from the source data to a specific vertex or edge label in HugeGraph.

| Name               | Type               | Required   | Default Value | Description                                                                                              |
| ------------------ |--------------------| ---------- | ------------- |----------------------------------------------------------------------------------------------------------|
| `type`             | String             | Yes        | -             | The type of graph element to map to. Must be `VERTEX` or `EDGE`.                                         |
| `label`            | String             | Yes        | -             | The label of the vertex or edge in HugeGraph.                                                            |
| `properties`       | `List<String>`       | No         | -             | A list of source field names for the vertex or edge.                                                     |
| `ttl`              | Long               | No         | -             | The time-to-live for the vertex or edge in seconds.                                                      |
| `ttlStartTime`     | String             | No         | -             | The start time for the TTL.                                                                              |
| `enableLabelIndex` | Boolean            | No         | `false`       | Whether to enable label index for this label.                                                            |
| `userdata`         | `Map<String, Object>` | No         | -             | User-defined data associated with the label.                                                             |
| `idStrategy`       | String             | For Vertex | -             | The ID generation strategy for vertices. Supported values: `PRIMARY_KEY`, `CUSTOMIZE_UUID`, `AUTOMATIC`. |
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
| `sortKeys`         | `List<String>`        | For Edge   | -             | A list of property keys  to sort edges with the same source and target vertices.                                                                                                  |

## Usage Examples

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
    graph_space = "default"
    selected_fields = ["name", "age"]
    schema_config = {
      type = "VERTEX"
      label = "person"
      idStrategy = "PRIMARY_KEY"
      idFields = ["name"]
      properties = ["name", "age"]
    }
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
    graph_space = "default"
    schema_config = {
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
      mapping = {
        fieldMapping = {
          person1_name = "name"
          person2_name = "name"
        }
      }
    }
  }
}
```

## Changelog

<ChangeLog />
