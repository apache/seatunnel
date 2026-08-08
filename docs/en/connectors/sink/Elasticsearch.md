import ChangeLog from '../changelog/connector-elasticsearch.md';

# Elasticsearch

> Elasticsearch sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Output data to `Elasticsearch`.

:::tip

Engine Supported: `ElasticSearch version is >= 2.x and <= 8.x`.

:::

## Key features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [x] [timer flush](../../introduction/concepts/connector-v2-features.md)

## Sink Options

| Name                     | Type    | Required | Default Value                     | Description                                                                                                                                                                                                                                       |
|--------------------------|---------|----------|-----------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hosts                    | Array   | Yes      | -                                 | Elasticsearch cluster HTTP address, format `host:port`, allowing multiple hosts, for example `["host1:9200", "host2:9200"]`.                                                                                                                       |
| index                    | String  | Yes      | -                                 | Elasticsearch index name. Supports placeholders that reference upstream field names, for example `seatunnel_${age}` (also requires `schema_save_mode = "IGNORE"`). The referenced field must exist in the upstream row, otherwise the placeholder is left as-is. |
| index_type               | String  | No       | -                                 | Elasticsearch index type. It is recommended not to specify this on Elasticsearch 6 and above.                                                                                                                                                      |
| primary_keys             | List    | No       | -                                 | Primary key fields used to generate the document `_id`. Required for CDC sources to produce deterministic document IDs.                                                                                                                            |
| key_delimiter            | String  | No       | `_`                               | Delimiter used to join `primary_keys` into a composite `_id`, for example `$` produces document IDs like `KEY1$KEY2$KEY3`.                                                                                                                          |
| schema_save_mode         | Enum    | No       | CREATE_SCHEMA_WHEN_NOT_EXIST      | How to handle the existing index structure on the target before the synchronization task starts. See `schema_save_mode` below.                                                                                                                    |
| data_save_mode           | Enum    | No       | APPEND_DATA                       | How to handle existing documents on the target before writing new data. See `data_save_mode` below.                                                                                                                                                |
| auth_type                | String  | No       | basic                             | Authentication method: `basic`, `api_key`, or `api_key_encoded`. Defaults to `basic` for backward compatibility.                                                                                                                                    |
| username                 | String  | No       | -                                 | Username for HTTP basic authentication (X-Pack username).                                                                                                                                                                                          |
| password                 | String  | No       | -                                 | Password for HTTP basic authentication (X-Pack password).                                                                                                                                                                                          |
| auth.api_key_id          | String  | No       | -                                 | API key ID for `auth_type = "api_key"`.                                                                                                                                                                                                            |
| auth.api_key             | String  | No       | -                                 | API key secret for `auth_type = "api_key"`.                                                                                                                                                                                                        |
| auth.api_key_encoded     | String  | No       | -                                 | Base64-encoded API key (`base64(id:api_key)`) for `auth_type = "api_key_encoded"`. Either `auth.api_key_id` + `auth.api_key` OR `auth.api_key_encoded` should be set, but not both.                                                                |
| max_retry_count          | Int     | No       | 3                                 | Maximum retry count for a single bulk request.                                                                                                                                                                                                     |
| max_batch_size           | Int     | No       | 10                                | Maximum number of documents included in a single bulk request.                                                                                                                                                                                     |
| tls_verify_certificate   | Boolean | No       | true                              | Enable certificate validation for HTTPS endpoints.                                                                                                                                                                                                 |
| tls_verify_hostname      | Boolean | No       | true                              | Enable hostname validation for HTTPS endpoints.                                                                                                                                                                                                   |
| tls_keystore_path        | String  | No       | -                                 | Path to the PEM or JKS key store. Must be readable by the user running SeaTunnel.                                                                                                                                                                  |
| tls_keystore_password    | String  | No       | -                                 | Key password for `tls_keystore_path`.                                                                                                                                                                                                              |
| tls_truststore_path      | String  | No       | -                                 | Path to the PEM or JKS trust store. Must be readable by the user running SeaTunnel.                                                                                                                                                                |
| tls_truststore_password  | String  | No       | -                                 | Key password for `tls_truststore_path`.                                                                                                                                                                                                            |
| vectorization_fields     | Array   | No       | -                                 | Field names that require vector conversion. Requires Elasticsearch 7.3 or later.                                                                                                                                                                    |
| vector_dimensions        | Int     | No       | 0                                 | Vector dimension for `vectorization_fields`. Requires Elasticsearch 7.3 or later.                                                                                                                                                                  |
| multi_table_sink_replica | Int     | No       | 1                                 | Number of sink writer replicas used per table in a multi-table sink job. Keep the default unless one table needs more parallelism.                                                                                                                  |
| common-options           |         | No       | -                                 | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.                                                                                                                          |

### hosts [Array]

Elasticsearch cluster HTTP address, format `host:port`, allowing multiple
hosts, for example `["host1:9200", "host2:9200"]`.

### index [String]

Elasticsearch index name. The value supports placeholders that reference
upstream field names, for example `seatunnel_${age}` (also requires
`schema_save_mode = "IGNORE"`). The referenced field must exist in the
upstream row, otherwise the placeholder is treated as a literal value.

### index_type [String]

Elasticsearch index type. It is recommended not to specify this on
Elasticsearch 6 and above.

### primary_keys [List]

Primary key fields used to generate the document `_id`. Required for CDC sources
to produce deterministic document IDs.

### key_delimiter [String]

Delimiter used to join `primary_keys` into a composite `_id`. The default `_`
produces IDs like `KEY1_KEY2_KEY3`. Use `$` to produce `KEY1$KEY2$KEY3`.

### auth_type [String]

Authentication method used by the connector:

- `basic` (default): HTTP Basic authentication using `username` and `password`.
- `api_key`: API key authentication using `auth.api_key_id` + `auth.api_key`.
- `api_key_encoded`: API key authentication using `auth.api_key_encoded`.

### username / password [String]

X-Pack credentials for HTTP basic authentication. Used when `auth_type =
"basic"` (default).

### auth.api_key_id / auth.api_key / auth.api_key_encoded [String]

API key credentials for `auth_type = "api_key"` or `auth_type =
"api_key_encoded"`. Either `auth.api_key_id` + `auth.api_key` OR
`auth.api_key_encoded` should be set, but not both.

### max_retry_count [Int]

Maximum retry count for a single bulk request when the upstream cluster is
unavailable.

### max_batch_size [Int]

Maximum number of documents included in a single bulk request.

### tls_verify_certificate [Boolean]

Enable certificate validation for HTTPS endpoints. Set to `false` to disable
certificate validation (typically only used in development).

### tls_verify_hostname [Boolean]

Enable hostname validation for HTTPS endpoints. Set to `false` to disable
hostname validation (typically only used in development).

### tls_keystore_path / tls_keystore_password [String]

Path to the PEM or JKS key store and its password. The file must be readable by
the user running SeaTunnel.

### tls_truststore_path / tls_truststore_password [String]

Path to the PEM or JKS trust store and its password. The file must be readable
by the user running SeaTunnel.

### vectorization_fields [Array]

Field names that require vector conversion. Requires Elasticsearch 7.3 or later.

### vector_dimensions [Int]

Vector dimension for `vectorization_fields`. Requires Elasticsearch 7.3 or later.

### multi_table_sink_replica [Int]

Number of sink writer replicas used per table in a multi-table sink job. Keep
the default value unless one table needs more sink writer parallelism.

### common options

Sink plugin common parameters, please refer to
[Sink Common Options](../common-options/sink-common-options.md) for details.

### schema_save_mode [Enum]

How to handle the existing index structure on the target before the
synchronization task starts.

- `RECREATE_SCHEMA`: Drop and recreate the index when it already exists.
- `CREATE_SCHEMA_WHEN_NOT_EXIST`: Create the index when it does not exist,
  skip when it already exists.
- `ERROR_WHEN_SCHEMA_NOT_EXIST`: Report an error when the index does not exist.
- `IGNORE`: Skip the index handling.

### data_save_mode [Enum]

How to handle existing documents on the target before writing new data.

- `DROP_DATA`: Preserve index structure and delete data.
- `APPEND_DATA`: Preserve index structure and preserve data.
- `ERROR_WHEN_DATA_EXISTS`: Report an error when there is existing data.

## Timer Flush on Zeta

This engine-level feature is supported only by Zeta. Spark and Flink do not
inject `FlushSignal` records. On Zeta, configure `sink.flush.interval` in the
`env` block to flush pending bulk requests before `max_batch_size` is reached.

:::tip

Elasticsearch timer flush does not provide 2PC exactly-once semantics. The
Elasticsearch Sink currently provides at-least-once delivery, and retries can
produce duplicate writes when document IDs are not deterministic.

:::

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 300000
  sink.flush.interval = 5000
}

sink {
  Elasticsearch {
    hosts = ["localhost:9200"]
    index = "seatunnel-index"
    max_batch_size = 10000
  }
}
```

## Task Example

### Simple

```hocon
sink {
    Elasticsearch {
        hosts = ["localhost:9200"]
        index = "seatunnel-${age}"
        schema_save_mode = "IGNORE"
    }
}
```

### Multi-table writing

```hocon
sink {
    Elasticsearch {
        hosts = ["localhost:9200"]
        index = "${table_name}"
        schema_save_mode = "IGNORE"
        multi_table_sink_replica = 1
    }
}
```

### Vector field writing

```hocon
sink {
    Elasticsearch {
        hosts = ["localhost:9200"]
        index = "${table_name}"
        schema_save_mode = "IGNORE"
        vectorization_fields = ["review_embedding"]
        vector_dimensions = 1024
    }
}
```

### CDC (Change Data Capture) event

```hocon
sink {
    Elasticsearch {
        hosts = ["localhost:9200"]
        index = "seatunnel-${age}"
        schema_save_mode = "IGNORE"
        primary_keys = ["key1", "key2"]
    }
}
```

### CDC multi-table writing

```hocon
sink {
    Elasticsearch {
        hosts = ["localhost:9200"]
        index = "${table_name}"
        schema_save_mode = "IGNORE"
        primary_keys = ["${primary_key}"]
    }
}
```

### SSL (Disable certificate validation)

```hocon
sink {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        username = "elastic"
        password = "elasticsearch"
        tls_verify_certificate = false
    }
}
```

### SSL (Disable hostname validation)

```hocon
sink {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        username = "elastic"
        password = "elasticsearch"
        tls_verify_hostname = false
    }
}
```

### SSL (Enable certificate validation)

```hocon
sink {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        username = "elastic"
        password = "elasticsearch"
        tls_keystore_path = "${your elasticsearch home}/config/certs/http.p12"
        tls_keystore_password = "${your password}"
    }
}
```

### Save Mode

```hocon
sink {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        username = "elastic"
        password = "elasticsearch"
        schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
        data_save_mode = "APPEND_DATA"
    }
}
```

## Schema Evolution

CDC collection supports a limited number of schema changes. The currently
supported schema changes include:

- Adding columns.

```hocon
env {
  parallelism = 5
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  read_limit.bytes_per_second = 7000000
  read_limit.rows_per_second = 400
}

source {
  MySQL-CDC {
    server-id = 5652-5657
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    schema-changes.enabled = true
  }
}

sink {
  Elasticsearch {
    hosts = ["https://elasticsearch:9200"]
    username = "elastic"
    password = "elasticsearch"
    tls_verify_certificate = false
    tls_verify_hostname = false
    index = "schema_change_index"
    index_type = "_doc"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

## Changelog

<ChangeLog />