import ChangeLog from '../changelog/connector-tablestore.md';

# Tablestore

> Tablestore sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Write SeaTunnel rows to Alibaba Cloud Tablestore.

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [ ] [cdc](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Data Type Mapping

| SeaTunnel type                         | Tablestore attribute column type | Tablestore primary key type |
|----------------------------------------|----------------------------------|-----------------------------|
| `INT`, `TINYINT`, `SMALLINT`, `BIGINT` | `INTEGER`                        | `INTEGER`                   |
| `FLOAT`, `DOUBLE`, `DECIMAL`           | `DOUBLE`                         | `STRING`                    |
| `STRING`, `DATE`, `TIME`, `TIMESTAMP`  | `STRING`                         | `STRING`                    |
| `BOOLEAN`                              | `BOOLEAN`                        | `STRING`                    |
| `BYTES`                                | `BINARY`                         | `BINARY`                    |

## Options

| name              | type   | required | default value | description                                      |
|-------------------|--------|----------|---------------|--------------------------------------------------|
| end_point         | string | yes      | -             | Tablestore endpoint.                             |
| instance_name     | string | yes      | -             | Tablestore instance name.                        |
| access_key_id     | string | yes      | -             | AccessKey ID used to access Tablestore.          |
| access_key_secret | string | yes      | -             | AccessKey secret used to access Tablestore.      |
| table             | string | yes      | -             | Target Tablestore table name.                    |
| primary_keys      | array  | yes      | -             | Primary key field names in the target table.     |
| schema            | config | yes      | -             | Input schema. Primary key fields must also exist in `schema.fields`. |
| batch_size        | int    | no       | 25            | Maximum number of rows written in one batch.     |
| common-options    | config | no       | -             | Sink common options.                             |

## Usage notes

- `primary_keys` can contain one or more primary key fields. These fields are written as Tablestore primary key columns; all other schema fields are written as normal attribute columns.
- The sink writes rows with Tablestore `RowPutChange` and `RowExistenceExpectation.IGNORE`. It does not delete rows when upstream sends `DELETE` row kinds.
- `batch_size` controls when buffered rows are flushed. The writer also flushes remaining rows when the job closes.
- Keep `access_key_id` and `access_key_secret` out of committed job files. Prefer runtime variable substitution or a secret manager supported by your deployment environment.

### common options [config]

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    row.num = 2
    schema = {
      fields {
        order_id = string
        user_id = string
        amount = double
      }
    }
    rows = [
      {
        fields = ["order-1", "user-1", 99.5]
      },
      {
        fields = ["order-2", "user-2", 20.0]
      }
    ]
  }
}

sink {
  Tablestore {
    end_point = "https://<instance>.<region>.ots.aliyuncs.com"
    instance_name = "<instance-name>"
    access_key_id = "${ACCESS_KEY_ID}"
    access_key_secret = "${ACCESS_KEY_SECRET}"
    table = "orders"
    primary_keys = ["order_id"]
    batch_size = 25
    schema = {
      fields {
        order_id = string
        user_id = string
        amount = double
      }
    }
  }
}
```

## Changelog

<ChangeLog />
