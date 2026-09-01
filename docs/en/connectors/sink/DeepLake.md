import ChangeLog from '../changelog/connector-deeplake.md';

# DeepLake

> Deep Lake sink connector

## Support These Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Description

The DeepLake sink appends SeaTunnel rows to a table hosted by the Deep Lake managed service. It uses the Deep Lake REST SQL API and does not require Python or a native Deep Lake client on SeaTunnel workers.

This connector supports managed workspaces reachable through the REST API. Local Deep Lake datasets and Python-only storage paths are not supported.

The sink accepts append-only input. `UPDATE_BEFORE`, `UPDATE_AFTER`, and `DELETE` rows fail instead of being silently appended with incorrect CDC semantics.

## Sink Options

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| api_url | string | no | `https://api.deeplake.ai` | Deep Lake REST API base URL. |
| api_key | string | yes | - | Deep Lake API key. Keep this value outside source control. |
| org_id | string | yes | - | Activeloop organization ID sent with each request. |
| workspace | string | yes | - | Workspace containing the destination table. |
| table | string | no | upstream table name | Destination table. |
| batch_size | int | no | `100` | Maximum rows sent to the batch query endpoint in one request. |
| connect_timeout_ms | int | no | `10000` | HTTP connection timeout in milliseconds. |
| socket_timeout_ms | int | no | `60000` | HTTP socket timeout in milliseconds. |
| schema_save_mode | enum | no | `CREATE_SCHEMA_WHEN_NOT_EXIST` | Schema handling mode. Supported values are `CREATE_SCHEMA_WHEN_NOT_EXIST`, `ERROR_WHEN_SCHEMA_NOT_EXIST`, and `IGNORE`. |
| multi_table_sink_replica | int | no | `1` | Parallel replica count for multi-table sink writing. |

### schema_save_mode

- `CREATE_SCHEMA_WHEN_NOT_EXIST`: sends `CREATE TABLE IF NOT EXISTS ... USING deeplake` before writing.
- `ERROR_WHEN_SCHEMA_NOT_EXIST`: validates the table with an empty query and fails when it is unavailable.
- `IGNORE`: assumes the table already exists and skips schema validation.

`RECREATE_SCHEMA` is rejected because dropping a managed dataset is destructive and unsafe when multiple sink writers start concurrently.

## Data Type Mapping

| SeaTunnel Data Type | Deep Lake SQL Type |
|---------------------|--------------------|
| BOOLEAN | BOOLEAN |
| TINYINT | SMALLINT |
| SMALLINT | SMALLINT |
| INT | INTEGER |
| BIGINT | BIGINT |
| FLOAT | REAL |
| DOUBLE | DOUBLE PRECISION |
| DECIMAL | NUMERIC(precision, scale) |
| STRING | TEXT |
| BYTES | BYTEA |
| DATE | DATE |
| TIME | TIME |
| TIMESTAMP | TIMESTAMP |
| TIMESTAMP_TZ | TIMESTAMPTZ |
| FLOAT_VECTOR | FLOAT4[] |
| BINARY_VECTOR | BYTEA |
| ARRAY | corresponding supported Deep Lake element type array |

`FLOAT16_VECTOR`, `BFLOAT16_VECTOR`, `SPARSE_FLOAT_VECTOR`, `MAP`, `ROW`, and arrays containing `BYTES` or `BINARY_VECTOR` are not supported in this first version. The connector fails during SQL generation rather than converting these values with a loss of precision or structure.

## Delivery Semantics

Rows are buffered in memory and written through Deep Lake's parameterized batch query endpoint. The buffer is cleared only after a successful HTTP response and is flushed when it reaches `batch_size`, on checkpoint preparation, and when an active writer closes. After a failed write, the writer becomes terminal and closing it does not retry the ambiguous batch.

The connector provides at-least-once delivery. A task failure after Deep Lake accepts a request but before SeaTunnel records the checkpoint can cause the batch to be sent again. A stable primary key can detect duplicates but does not make retries exactly-once. Deduplicate before or after this sink when duplicate rows are not acceptable.

The connector maps each input field to the Deep Lake column with the same name. It does not generate a synthetic record ID. Define a primary key in the upstream schema when the destination needs a stable application-level identifier.

## Task Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 10
    schema = {
      fields {
        document_id = bigint
        content = string
        score = double
      }
    }
    plugin_output = "documents"
  }
}

sink {
  DeepLake {
    plugin_input = "documents"
    api_key = "${DEEPLAKE_API_KEY}"
    org_id = "${DEEPLAKE_ORG_ID}"
    workspace = "research"
    table = "documents"
    batch_size = 100
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
  }
}
```

## Changelog

<ChangeLog />
