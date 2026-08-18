import ChangeLog from '../changelog/connector-lance.md';

# Lance

> Lance sink connector

## Support These Engines

> Spark 3.4 and later<br/>
> Flink is not supported by this connector<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Description

The Lance sink writes SeaTunnel rows into a Lance dataset. It can create a Lance table from the incoming SeaTunnel schema and then append or create data according to the configured Lance write mode.

The connector currently works with directory-based Lance namespaces.
It provides a sink only; there is no Lance source connector.

## Using Dependency

```xml
<dependency>
    <groupId>com.lancedb</groupId>
    <artifactId>lance-core</artifactId>
    <version>0.33.0</version>
</dependency>

<dependency>
    <groupId>com.lancedb</groupId>
    <artifactId>lance-namespace-core</artifactId>
    <version>0.0.14</version>
</dependency>
```

## Sink Options

| Name                               | Type    | Required | Default       | Description                                                                                      |
|------------------------------------|---------|----------|---------------|--------------------------------------------------------------------------------------------------|
| dataset_path                       | string  | no       | /test.lance   | Lance dataset path. For a directory namespace, this is the local path used by the Lance dataset. |
| namespace_type                     | string  | no       | dir           | Lance namespace type. Currently only `dir` is supported.                                         |
| namespace_id                       | string  | no       | ""            | Lance namespace ID.                                                                              |
| namespace_ids                      | list    | no       | []            | Lance namespace path segments used when resolving the target table namespace.                     |
| root_namespace_path                | string  | no       | /tmp          | Root path used by the Lance namespace.                                                           |
| table                              | string  | no       | test          | Target Lance table name. If set, it overrides the upstream table name.                            |
| lance.write.max-rows-per-file      | int     | no       | 10            | Maximum number of rows written to one Lance file.                                                 |
| lance.write.max-rows-per-group     | int     | no       | 20            | Maximum number of rows written to one Lance row group.                                            |
| lance.write.max-bytes-per-file     | long    | no       | 20480         | Maximum number of bytes written to one Lance file.                                                |
| lance.write.mode                   | string  | no       | CREATE        | Lance write mode. The value is passed to Lance `WriteParams.WriteMode`.                           |
| lance.write.enable.stable.row.ids  | boolean | no       | true          | Whether to enable stable row IDs when writing Lance data.                                         |
| lance.write.storage.options        | map     | no       | {}            | Extra storage options passed to Lance.                                                           |
| multi_table_sink_replica           | int     | no       | 1             | Parallel replica count for multi-table sink writing.                                              |

### dataset_path

The directory or dataset path where Lance data is stored. In local directory mode, make sure the SeaTunnel runtime can create and write to this path.

### namespace_type

The Lance namespace type. Currently the connector supports `dir`.

### namespace_id

The namespace name used by the directory namespace implementation. In local
directory mode, it can be a simple name such as `root`.

### namespace_ids

Additional namespace path segments used when resolving the table namespace.
Leave it empty when writing directly under the root namespace.

### root_namespace_path

The root directory for the Lance namespace. The runtime user must have permission
to create and write files under this path.

### table

The target Lance table name. When it is not set, the connector uses the upstream table name if one is available. The default option value is `test`.

### lance.write.mode

Controls how Lance writes the dataset. The default is `CREATE`. Use a value
supported by Lance `WriteParams.WriteMode`: `CREATE`, `APPEND`, or `OVERWRITE`.

### lance.write.enable.stable.row.ids

Whether to enable stable row IDs when writing Lance data. The connector reads this option into `LanceSinkConfig.enableStableRowIds` and exposes it via `getEnableStableRowIds()`, but **the value is currently parsed but not yet applied to the underlying Lance `WriteParams`** in `LanceSinkWriter.initializeDataset()` — toggling it has no observable effect on the write path today. This is a known gap pending a follow-up connector change.

### lance.write.storage.options

Passes extra storage parameters to Lance as key-value pairs.

Example:

```hocon
lance.write.storage.options = {
  key1 = "value1"
  key2 = "value2"
}
```

### multi_table_sink_replica

Replica count used by the multi-table sink routing mechanism. Increase it when
a single multi-table job writes to many Lance tables and the default single
replica becomes a bottleneck. See [Sink Common Options](../common-options/sink-common-options.md).

## Data Type Mapping

Lance uses the Apache Arrow type system. The sink creates the Lance schema from the incoming SeaTunnel schema. The current mapping narrows every integer SeaTunnel type (`TINYINT`, `SMALLINT`, `INT`, `BIGINT`) to Arrow `int32`, so `BIGINT` values outside the signed 32-bit range are truncated.

| SeaTunnel Data Type | Lance / Arrow Data Type |
|---------------------|-------------------------|
| BOOLEAN             | bool                    |
| TINYINT             | int32                   |
| SMALLINT            | int32                   |
| INT                 | int32                   |
| BIGINT              | int32 (values outside the signed 32-bit range are truncated) |
| FLOAT               | float32                 |
| DOUBLE              | float64                 |
| DECIMAL             | decimal128              |
| NULL                | null                    |
| BYTES               | binary                  |
| DATE                | date32                  |
| TIME                | time32 (millisecond precision) |
| TIMESTAMP           | timestamp (microsecond, Asia/Shanghai timezone) |
| STRING              | utf8                    |
| ARRAY               | list                    |
| MAP                 | map                     |

:::tip

The sink does not interpret `UPDATE` / `DELETE` row kinds as CDC operations — every upstream row is appended to the Lance dataset using the configured `lance.write.mode`. In streaming mode, the writer flushes the in-memory row buffer to Lance on every checkpoint.

:::

## Task Example

### Write FakeSource Data To Lance

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 100
    schema = {
      fields {
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(30, 8)"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
    plugin_output = "fake"
  }
}

sink {
  Lance {
    dataset_path = "/tmp/seatunnel_mnt/lanceTest/lance_sink_table"
    namespace_type = "dir"
    namespace_id = "root"
    table = "lance_sink_table"
  }
}
```

### Append Mode With Larger File Fragments

`APPEND` mode keeps the existing dataset and adds new rows. Increase
`lance.write.max-rows-per-file` and `lance.write.max-bytes-per-file` to reduce
the number of Lance fragments when appending large batches.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 1000000
    schema = {
      fields {
        c_string = string
        c_int = int
      }
    }
    plugin_output = "fake"
  }
}

sink {
  Lance {
    dataset_path = "/tmp/seatunnel_mnt/lanceTest/lance_sink_table"
    namespace_type = "dir"
    namespace_id = "root"
    table = "lance_sink_table"
    lance.write.mode = "APPEND"
    lance.write.max-rows-per-file = 100000
    lance.write.max-rows-per-group = 5000
    lance.write.max-bytes-per-file = 134217728
  }
}
```

### Streaming Append With Checkpoint Flush

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
  checkpoint.interval = 30000
}

source {
  FakeSource {
    row.num = 1000
    schema = {
      fields {
        c_string = string
        c_int = int
      }
    }
    plugin_output = "fake_stream"
  }
}

sink {
  Lance {
    plugin_input = "fake_stream"
    dataset_path = "/tmp/seatunnel_mnt/lanceTest/lance_sink_table"
    namespace_type = "dir"
    namespace_id = "root"
    table = "lance_sink_table"
    lance.write.mode = "APPEND"
  }
}
```

## Changelog

<ChangeLog />
