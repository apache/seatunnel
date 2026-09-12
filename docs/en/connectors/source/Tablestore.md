import ChangeLog from '../changelog/connector-tablestore.md';

# Tablestore

> Tablestore source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Read full and incremental data from Alibaba Cloud Tablestore. The source uses Tablestore Tunnel in `BaseAndStream` mode, so it can read existing data first and then consume later changes.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Options

| name              | type   | required | default value | description                                                                 |
|-------------------|--------|----------|---------------|-----------------------------------------------------------------------------|
| end_point         | string | yes      | -             | Tablestore endpoint, for example `https://<instance>.<region>.ots.aliyuncs.com`. |
| instance_name     | string | yes      | -             | Tablestore instance name.                                                    |
| access_key_id     | string | yes      | -             | AccessKey ID used to access Tablestore.                                      |
| access_key_secret | string | yes      | -             | AccessKey secret used to access Tablestore.                                  |
| table             | string | yes      | -             | Tablestore table name. Multiple tables can be separated by commas.           |
| primary_keys      | array  | yes      | -             | Primary key names. For multiple source tables, configure one primary key name for each table in the same order as `table`. |
| schema            | config | yes      | -             | Output schema. For details, see [Schema Feature](../../introduction/concepts/schema-feature.md). |

## Usage notes

- `job.mode = "BATCH"` reads bounded data. `job.mode = "STREAMING"` keeps consuming incremental records after the existing data is read.
- When `table` contains multiple table names, `primary_keys` must contain the same number of entries. For example, `table = "orders,users"` can use `primary_keys = ["id", "id"]` when both tables use `id` as the primary key field.
- Multi-table reads use one `schema` block for the source, so the listed tables should have compatible output fields.
- The source emits `INSERT`, `UPDATE_AFTER`, and `DELETE` row kinds according to Tablestore stream records.
- Keep `access_key_id` and `access_key_secret` out of committed job files. Prefer runtime variable substitution or a secret manager supported by your deployment environment.

## Example

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
}

source {
  Tablestore {
    end_point = "https://<instance>.<region>.ots.aliyuncs.com"
    instance_name = "<instance-name>"
    access_key_id = "${ACCESS_KEY_ID}"
    access_key_secret = "${ACCESS_KEY_SECRET}"
    table = "orders"
    primary_keys = ["order_id"]
    schema = {
      fields {
        order_id = string
        user_id = string
        amount = double
        updated_at = string
      }
    }
  }
}

sink {
  Console {}
}
```

### Multiple table example

```hocon
env {
  parallelism = 2
  job.mode = "STREAMING"
}

source {
  Tablestore {
    end_point = "https://<instance>.<region>.ots.aliyuncs.com"
    instance_name = "<instance-name>"
    access_key_id = "${ACCESS_KEY_ID}"
    access_key_secret = "${ACCESS_KEY_SECRET}"
    table = "orders,users"
    primary_keys = ["id", "id"]
    schema = {
      fields {
        id = string
        value = string
        updated_at = string
      }
    }
  }
}

sink {
  Console {}
}
```

## Changelog

<ChangeLog />
