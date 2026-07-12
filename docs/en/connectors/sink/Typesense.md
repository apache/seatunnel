import ChangeLog from '../changelog/connector-typesense.md';

# Typesense

## Description

Writes SeaTunnel rows to a Typesense collection. The connector can create the target collection
when needed, clear existing documents before writing, and generate document `id` values from one
or more primary key fields.

## Key Features

- [ ] [Exactly Once](../../introduction/concepts/connector-v2-features.md)
- [x] [CDC](../../introduction/concepts/connector-v2-features.md)
- [x] [Multiple Table Sink](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Options

| Name                     | Type   | Required | Default                      | Description                                                                                          |
|--------------------------|--------|----------|------------------------------|------------------------------------------------------------------------------------------------------|
| hosts                    | array  | Yes      | -                            | Typesense node addresses in `host:port` format. Multiple hosts are supported.                        |
| collection               | string | Yes      | -                            | Target collection name.                                                                              |
| schema_save_mode         | string | Yes      | CREATE_SCHEMA_WHEN_NOT_EXIST | How to handle the target collection schema before writing.                                           |
| data_save_mode           | string | Yes      | APPEND_DATA                  | How to handle existing documents before writing.                                                     |
| primary_keys             | array  | No       | -                            | Source fields used to build the Typesense document `id`.                                             |
| key_delimiter            | string | No       | `_`                          | Delimiter used when `primary_keys` contains more than one field.                                     |
| api_key                  | string | Yes      | -                            | Typesense API key.                                                                                   |
| max_retry_count          | int    | No       | 3                            | Maximum retry count for one bulk request.                                                            |
| max_batch_size           | int    | No       | 10                           | Maximum number of documents sent in one bulk request.                                                |
| multi_table_sink_replica | int    | No       | -                            | Number of sink replicas used by the common multi-table sink routing mechanism.                       |
| common-options           |        | No       | -                            | Common sink options.                                                                                 |

### hosts [array]

The access address for Typesense, formatted as `host:port`, e.g., `["typesense-01:8108"]`.

### collection [string]

The name of the collection to write to, e.g., "seatunnel".

### primary_keys [array]

Primary key fields used to generate the document `id`. When more than one field is configured,
the connector joins their values with `key_delimiter`.

### key_delimiter [string]

Sets the delimiter for composite keys (default is `_`).

### api_key [string]

The `api_key` for secure access to Typesense.

### max_retry_count [int]

The maximum number of retry attempts for one batch request.

### max_batch_size [int]

The maximum number of documents sent in one batch.

### multi_table_sink_replica [int]

Common multi-table sink option. Configure it when a multi-table job needs more sink replicas for
the Typesense writer.

### common options

Common parameters for Sink plugins. Refer to [Common Sink Options](../common-options/sink-common-options.md) for more details.

### schema_save_mode

Choose how to handle the target-side schema before starting the synchronization task:
- `RECREATE_SCHEMA`: Creates the table if it doesn’t exist, and deletes and recreates it if it does.
- `CREATE_SCHEMA_WHEN_NOT_EXIST`: Creates the table if it doesn’t exist, skips creation if it does.
- `ERROR_WHEN_SCHEMA_NOT_EXIST`: Throws an error if the table doesn’t exist.

Typesense collection creation uses the upstream SeaTunnel schema. Configure `primary_keys` when the
generated document `id` must be stable across repeated writes.

### data_save_mode

Choose how to handle existing data on the target side before starting the synchronization task:
- `DROP_DATA`: Retains the database structure but deletes the data.
- `APPEND_DATA`: Retains both the database structure and the data.
- `ERROR_WHEN_DATA_EXISTS`: Throws an error if data exists.

## Task Example

### Write Documents With Primary Keys

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 5
    plugin_output = "typesense_test_table"
    schema {
      fields {
        company_name = string
        num = long
        id = string
        num_employees = int
        flag = boolean
      }
    }
  }
}

sink {
  Typesense {
    plugin_input = "typesense_test_table"
    hosts = ["localhost:8108"]
    collection = "typesense_test_collection"
    api_key = "xyz"
    primary_keys = ["num_employees", "num"]
    key_delimiter = "="
    max_retry_count = 3
    max_batch_size = 10
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

### Read From Typesense And Write To Another Collection

```bash
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Typesense {
    hosts = ["localhost:8108"]
    collection = "typesense_source_collection"
    api_key = "xyz"
    query = "q=*&filter_by=c_row.c_int:>10"
    plugin_output = "typesense_test_table"
    schema = {
      fields {
        company_name_list = array<string>
        company_name = string
        num_employees = long
        country = string
        id = string
        c_row = {
          c_int = int
          c_string = string
          c_array_int = array<int>
        }
      }
    }
  }
}

sink {
  Typesense {
    plugin_input = "typesense_test_table"
    hosts = ["localhost:8108"]
    collection = "typesense_sink_collection"
    api_key = "xyz"
    primary_keys = ["num_employees", "id"]
    key_delimiter = "="
    max_retry_count = 3
    max_batch_size = 10
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

## Changelog

<ChangeLog />
