import ChangeLog from '../changelog/connector-easysearch.md';

# INFINI Easysearch

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

A sink plugin used to send data to `INFINI Easysearch`.

## Using Dependency

> Dependency [easysearch-client](https://central.sonatype.com/artifact/com.infinilabs/easysearch-client)

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)

:::tip

Engine Supported

* Supported all versions released by [INFINI Easysearch](https://www.infini.com/download/?product=easysearch).

:::

## Data Type Mapping

|    Easysearch Data Type     | SeaTunnel Data Type  |
|-----------------------------|----------------------|
| STRING<br/>KEYWORD<br/>TEXT | STRING               |
| BOOLEAN                     | BOOLEAN              |
| BYTE                        | BYTE                 |
| SHORT                       | SHORT                |
| INTEGER                     | INT                  |
| LONG                        | LONG                 |
| FLOAT<br/>HALF_FLOAT        | FLOAT                |
| DOUBLE                      | DOUBLE               |
| DATE                        | LOCAL_DATE_TIME_TYPE |

## Sink Options

|          name          |  type   | required | default value | description                                                                                                       |
|------------------------|---------|----------|---------------|-------------------------------------------------------------------------------------------------------------------|
| hosts                  | array   | yes      | -             | Easysearch HTTP cluster addresses in `host:port` format, for example `["host1:9200", "host2:9200"]`.              |
| index                  | string  | yes      | -             | Easysearch index name. Supports placeholders such as `seatunnel_${age}`.                                          |
| primary_keys           | list    | no       | -             | Primary key fields used to build the document `_id`. Configure when writing CDC rows that need upsert/delete.      |
| key_delimiter          | string  | no       | `_`           | Delimiter used to join composite key fields when building `_id`.                                                 |
| username               | string  | no       | -             | Username for secured Easysearch clusters.                                                                        |
| password               | string  | no       | -             | Password for secured Easysearch clusters.                                                                        |
| max_retry_count        | int     | no       | 3             | Maximum retry count for one bulk request.                                                                        |
| max_batch_size         | int     | no       | 10            | Maximum number of documents buffered into one bulk request.                                                      |
| tls_verify_certificate | boolean | no       | true          | Whether to validate HTTPS certificates.                                                                          |
| tls_verify_hostname    | boolean | no       | true          | Whether to validate HTTPS host names.                                                                            |
| tls_keystore_path      | string  | no       | -             | Path to the PEM or JKS key store.                                                                                |
| tls_keystore_password  | string  | no       | -             | Password for the configured key store.                                                                           |
| tls_truststore_path    | string  | no       | -             | Path to the PEM or JKS trust store.                                                                              |
| tls_truststore_password | string | no       | -             | Password for the configured trust store.                                                                         |
| schema_save_mode       | enum    | no       | CREATE_SCHEMA_WHEN_NOT_EXIST | How to handle the target index before the synchronization task starts. See `schema_save_mode` below. |
| data_save_mode         | enum    | no       | APPEND_DATA   | How to handle existing target data before the synchronization task starts. See `data_save_mode` below.           |
| common-options         |         | no       | -             | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md).   |

### hosts [array]

`INFINI Easysearch` cluster http address, the format is `host:port` , allowing multiple hosts to be specified. Such as `["host1:9200", "host2:9200"]`.

### index [string]

`INFINI Easysearch` index name. The index can contain field placeholders, such as `seatunnel_${age}`. The referenced field must exist in the input row; otherwise the value is treated as a normal literal index name.

### primary_keys [list]

Primary key fields used to generate the document `_id`. Configure this option when writing CDC rows that need update or delete semantics.

### key_delimiter [string]

Delimiter for composite keys ("_" by default), e.g., "$" would result in document `_id` "KEY1$KEY2$KEY3".

### username [string]

security username

### password [string]

security password

### max_retry_count [int]

Maximum retry count for one bulk request.

### max_batch_size [int]

Maximum number of documents buffered in one bulk request.

### tls_verify_certificate [boolean]

Enable certificates validation for HTTPS endpoints

### tls_verify_hostname [boolean]

Enable hostname validation for HTTPS endpoints

### tls_keystore_path [string]

The path to the PEM or JKS key store. This file must be readable by the operating system user running SeaTunnel.

### tls_keystore_password [string]

The key password for the key store specified

### tls_truststore_path [string]

The path to PEM or JKS trust store. This file must be readable by the operating system user running SeaTunnel.

### tls_truststore_password [string]

The key password for the trust store specified

### schema_save_mode [enum]

Choose how to handle the target-side schema before starting the synchronization task:
- `RECREATE_SCHEMA`: Creates the table if it doesn't exist, and deletes and recreates it if it does.
- `CREATE_SCHEMA_WHEN_NOT_EXIST`: Creates the table if it doesn't exist, skips creation if it does.
- `ERROR_WHEN_SCHEMA_NOT_EXIST`: Throws an error if the table doesn't exist.
- `IGNORE`: Ignores schema handling.

### data_save_mode [enum]

Choose how to handle the target-side data before starting the synchronization task:
- `DROP_DATA`: Preserves the database structure and deletes the data.
- `APPEND_DATA`: Preserves the database structure and the data.
- `ERROR_WHEN_DATA_EXISTS`: Reports an error when data exists.

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.

## Examples

### Write To One Index

```hocon
sink {
  Easysearch {
    hosts = ["localhost:9200"]
    index = "seatunnel_index"
    max_batch_size = 100
  }
}
```

### Write To Dynamic Index

```hocon
sink {
  Easysearch {
    hosts = ["localhost:9200"]
    index = "seatunnel_${age}"
  }
}
```

### CDC Event

```hocon
sink {
  Easysearch {
    hosts = ["localhost:9200"]
    index = "seatunnel_${age}"
    primary_keys = ["key1", "key2"]
  }
}
```

### Multiple Table Sink

When upstream rows carry table identifiers (for example via a multi-table source), use `${table_name}` in the index
name so rows from different upstream tables are routed to different Easysearch indices.

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    tables_configs = [
      {
        schema = {
          table = "db.schema.table_a"
          fields {
            id = int
            name = string
          }
        }
        rows = [
          { kind = INSERT, fields = [1, "alice"] }
        ]
      },
      {
        schema = {
          table = "db.schema.table_b"
          fields {
            id = int
            amount = double
          }
        }
        rows = [
          { kind = INSERT, fields = [2, 6.3] }
        ]
      }
    ]
  }
}

sink {
  Easysearch {
    hosts = ["localhost:9200"]
    index = "st_${table_name}"
    primary_keys = ["id"]
  }
}
```

SSL (Disable certificates validation)

```hocon
sink {
    Easysearch {
        hosts = ["https://localhost:9200"]
        username = "admin"
        password = "admin"

        tls_verify_certificate = false
    }
}
```

SSL (Disable hostname validation)

```hocon
sink {
    Easysearch {
        hosts = ["https://localhost:9200"]
        username = "admin"
        password = "admin"

        tls_verify_hostname = false
    }
}
```

SSL (Enable certificates validation)

```hocon
sink {
    Easysearch {
        hosts = ["https://localhost:9200"]
        username = "admin"
        password = "admin"

        tls_keystore_path = "${your Easysearch home}/config/certs/http.p12"
        tls_keystore_password = "${your password}"
    }
}
```

### Save Mode

```hocon
sink {
    Easysearch {
        hosts = ["https://localhost:9200"]
        username = "admin"
        password = "admin"

        index = "seatunnel_index"
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode = "APPEND_DATA"
    }
}
```

## Changelog

<ChangeLog />
