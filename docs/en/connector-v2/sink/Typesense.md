# Typesense

## Description

Outputs data to `Typesense`.

## Key Features

- [ ] [Exactly Once](../../concept/connector-v2-features.md)
- [x] [CDC](../../concept/connector-v2-features.md)

## Options

|       Name       |  Type  | Required |        Default Value         |
|------------------|--------|----------|------------------------------|
| hosts            | array  | Yes      | -                            |
| collection       | string | Yes      | -                            |
| schema_save_mode | string | Yes      | CREATE_SCHEMA_WHEN_NOT_EXIST |
| data_save_mode   | string | Yes      | APPEND_DATA                  |
| primary_keys     | array  | No       |                              |
| key_delimiter    | string | No       | `_`                          |
| api_key          | string | No       |                              |
| max_retry_count  | int    | No       | 3                            |
| max_batch_size   | int    | No       | 10                           |
| common-options   |        | No       | -                            |

### hosts [array]

The access address for Typesense, formatted as `host:port`, e.g., `["typesense-01:8108"]`.

### collection [string]

The name of the collection to write to, e.g., "seatunnel".

### primary_keys [array]

Primary key fields used to generate the document `id`.

### key_delimiter [string]

Sets the delimiter for composite keys (default is `_`).

### api_key [config]

The `api_key` for secure access to Typesense.

### max_retry_count [int]

The maximum number of retry attempts for batch requests.

### max_batch_size [int]

The maximum size of document batches.

### common options

Common parameters for Sink plugins. Refer to [Common Sink Options](../source-common-options.md) for more details.

### schema_save_mode

Choose how to handle the target-side schema before starting the synchronization task:
- `RECREATE_SCHEMA`: Creates the table if it doesn’t exist, and deletes and recreates it if it does.
- `CREATE_SCHEMA_WHEN_NOT_EXIST`: Creates the table if it doesn’t exist, skips creation if it does.
- `ERROR_WHEN_SCHEMA_NOT_EXIST`: Throws an error if the table doesn’t exist.

### data_save_mode

Choose how to handle existing data on the target side before starting the synchronization task:
- `DROP_DATA`: Retains the database structure but deletes the data.
- `APPEND_DATA`: Retains both the database structure and the data.
- `ERROR_WHEN_DATA_EXISTS`: Throws an error if data exists.

## Example

Simple example:

```bash
sink {
    Typesense {
        source_table_name = "typesense_test_table"
        hosts = ["localhost:8108"]
        collection = "typesense_to_typesense_sink_with_query"
        max_retry_count = 3
        max_batch_size = 10
        api_key = "xyz"
        primary_keys = ["num_employees","id"]
        key_delimiter = "="
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode = "APPEND_DATA"
    }
}
```

