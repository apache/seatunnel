import ChangeLog from '../changelog/connector-lance.md';

# Lance

> Lance sink connector

## Support These Engines

> Spark 3.4 and later<br/>
> Flink is not supported by this connector<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)

## Description

The Lance sink writes SeaTunnel rows into a Lance dataset. It can create a Lance table from the incoming SeaTunnel schema and then append or create data according to the configured Lance write mode.

The connector currently works with directory-based Lance namespaces.

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
| dataset_path                       | string  | yes      | /test.lance   | Lance dataset path. For a directory namespace, this is the local path used by the Lance dataset. |
| namespace_type                     | string  | yes      | dir           | Lance namespace type. Currently only `dir` is supported.                                         |
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

### table

The target Lance table name. When it is not set, the connector uses the upstream table name if one is available. The default option value is `test`.

### lance.write.mode

Controls how Lance writes the dataset. The default is `CREATE`. Use a value supported by Lance `WriteParams.WriteMode`.

### lance.write.storage.options

Passes extra storage parameters to Lance as key-value pairs.

Example:

```hocon
lance.write.storage.options = {
  key1 = "value1"
  key2 = "value2"
}
```

## Data Type Mapping

Lance uses the Apache Arrow type system. The sink creates the Lance schema from the incoming SeaTunnel schema.

| SeaTunnel Data Type | Lance / Arrow Data Type |
|---------------------|-------------------------|
| BOOLEAN             | bool                    |
| TINYINT             | int32                   |
| SMALLINT            | int32                   |
| INT                 | int32                   |
| BIGINT              | int32                   |
| FLOAT               | float64                 |
| DOUBLE              | float64                 |
| DECIMAL             | decimal128              |
| BYTES               | binary                  |
| DATE                | date32                  |
| TIME                | time32                  |
| TIMESTAMP           | timestamp               |
| STRING              | utf8                    |
| ARRAY               | list                    |
| MAP                 | map                     |

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

## Changelog

<ChangeLog />
