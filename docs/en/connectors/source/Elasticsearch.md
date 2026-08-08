import ChangeLog from '../changelog/connector-elasticsearch.md';

# Elasticsearch

> Elasticsearch source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Used to read data from Elasticsearch.

Supports Elasticsearch versions `>= 2.x` and `<= 8.x`.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)

## Source Options

| Name                     | Type    | Required | Default Value | Description                                                                                                                                                                                                                                                |
|--------------------------|---------|----------|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hosts                    | Array   | Yes      | -             | Elasticsearch cluster HTTP address, format `host:port`, allowing multiple hosts, for example `["host1:9200", "host2:9200"]`.                                                                                                                                |
| auth_type                | String  | No       | basic         | Authentication method: `basic`, `api_key`, or `api_key_encoded`. Defaults to `basic` for backward compatibility.                                                                                                                                           |
| username                 | String  | No       | -             | Username for HTTP basic authentication (X-Pack username).                                                                                                                                                                                                   |
| password                 | String  | No       | -             | Password for HTTP basic authentication (X-Pack password).                                                                                                                                                                                                   |
| auth.api_key_id          | String  | No       | -             | API key ID for `auth_type = "api_key"`.                                                                                                                                                                                                                     |
| auth.api_key             | String  | No       | -             | API key secret for `auth_type = "api_key"`.                                                                                                                                                                                                                 |
| auth.api_key_encoded     | String  | No       | -             | Base64-encoded API key (`base64(id:api_key)`) for `auth_type = "api_key_encoded"`. Either `auth.api_key_id` + `auth.api_key` OR `auth.api_key_encoded` should be set, but not both.                                                                       |
| index                    | String  | No       | -             | Elasticsearch index name. Required when `index_list` is not configured. Supports `*` fuzzy matching.                                                                                                                                                       |
| index_list               | Array   | No       | -             | Define a multi-index synchronization task. Use this when different indexes need their own `query`, `source`, `schema`, or paging options.                                                                                                                    |
| source                   | Array   | No       | -             | Fields to read from the document. The `_id` field can be exposed by listing it explicitly. If not set, fields are automatically derived from the index mapping.                                                                                            |
| query                    | Json    | No       | `{"match_all": {}}` | Elasticsearch DSL query used to control which documents are read.                                                                                                                                                                                       |
| search_type              | Enum    | No       | DSL           | Query type, available values: `DSL` (default) or `SQL`.                                                                                                                                                                                                     |
| search_api_type          | Enum    | No       | SCROLL        | Pagination API type: `SCROLL` (default) or `PIT` (Point-in-Time).                                                                                                                                                                                            |
| sql_query                | String  | No       | -             | SQL query, required when `search_type = "SQL"`.                                                                                                                                                                                                            |
| scroll_time              | String  | No       | 1m            | Amount of time Elasticsearch keeps the search context alive for scroll requests.                                                                                                                                                                           |
| scroll_size              | Int     | No       | 100           | Maximum number of hits returned per Elasticsearch scroll request.                                                                                                                                                                                          |
| pit_keep_alive           | Long    | No       | 60000         | Lifetime (in milliseconds) of a PIT search context.                                                                                                                                                                                                         |
| pit_batch_size           | Int     | No       | 100           | Maximum number of hits returned per PIT search request.                                                                                                                                                                                                     |
| tls_verify_certificate   | Boolean | No       | true          | Enable certificate validation for HTTPS endpoints.                                                                                                                                                                                                          |
| tls_verify_hostname      | Boolean | No       | true          | Enable hostname validation for HTTPS endpoints.                                                                                                                                                                                                            |
| array_column             | Map     | No       | -             | Mark fields as array columns because Elasticsearch has no array type. Example: `{c_array = "array<tinyint>"}`.                                                                                                                                              |
| tls_keystore_path        | String  | No       | -             | Path to the PEM or JKS key store. Must be readable by the user running SeaTunnel.                                                                                                                                                                            |
| tls_keystore_password    | String  | No       | -             | Key password for `tls_keystore_path`.                                                                                                                                                                                                                       |
| tls_truststore_path      | String  | No       | -             | Path to the PEM or JKS trust store. Must be readable by the user running SeaTunnel.                                                                                                                                                                         |
| tls_truststore_password  | String  | No       | -             | Key password for `tls_truststore_path`.                                                                                                                                                                                                                     |
| runtime_fields           | Array   | No       | -             | Runtime fields computed at query time (Elasticsearch 7.11+). See [Runtime Fields](#runtime-fields) below.                                                                                                                                                  |
| slice_max                | Int     | No       | 1             | Split a single index into multiple slices for parallel reads. Only effective for `SCROLL` / `PIT`. `SCROLL` slicing requires ES 5.0+; `PIT` slicing requires ES 7.10+. Ignored when `search_type = "SQL"`.                                                  |
| common-options           |         | No       | -             | Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.                                                                                                                           |

### hosts [Array]

Elasticsearch cluster HTTP address, format `host:port`, allowing multiple
hosts, for example `["host1:9200", "host2:9200"]`.

## Authentication

The Elasticsearch connector supports multiple authentication methods. Choose
the appropriate one based on your Elasticsearch security configuration.

### auth_type [String]

Specifies the authentication method to use. Supported values:

- `basic` (default): HTTP Basic authentication using `username` and `password`.
- `api_key`: API key authentication using `auth.api_key_id` + `auth.api_key`.
- `api_key_encoded`: API key authentication using `auth.api_key_encoded`.

If not specified, defaults to `basic` for backward compatibility.

### Basic Authentication

#### username [String]

Username for HTTP basic authentication (X-Pack username).

#### password [String]

Password for HTTP basic authentication (X-Pack password).

```hocon
source {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        auth_type = "basic"
        username = "elastic"
        password = "your_password"
        index = "my_index"
    }
}
```

### API Key Authentication

#### auth.api_key_id [String]

The API key ID generated by Elasticsearch.

#### auth.api_key [String]

The API key secret generated by Elasticsearch.

#### auth.api_key_encoded [String]

Base64-encoded API key in the format `base64(id:api_key)`. Alternative to
specifying `auth.api_key_id` and `auth.api_key` separately.

**Note:** You can use either `auth.api_key_id` + `auth.api_key` OR
`auth.api_key_encoded`, but not both.

```hocon
source {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        auth_type = "api_key"
        auth.api_key_id = "your_api_key_id"
        auth.api_key = "your_api_key_secret"
        index = "my_index"
    }
}
```

```hocon
source {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        auth_type = "api_key_encoded"
        auth.api_key_encoded = "eW91cl9hcGlfa2V5X2lkOnlvdXJfYXBpX2tleV9zZWNyZXQ="
        index = "my_index"
    }
}
```

### index [String]

Elasticsearch index name. Supports `*` fuzzy matching.

Either `index` or `index_list` must be configured. Use `index` for a single
index or an index pattern, and use `index_list` when different indexes need
their own `query`, `source`, `schema`, or paging options.

### source [Array]

Fields to read from the document. You can expose the document `_id` by
listing it explicitly. When sinking `_id` to another index, an alias is
required because of Elasticsearch limits. If `source` is not configured, the
fields are automatically derived from the index mapping.

### query [Json]

Elasticsearch DSL used to control the range of data read. The default is
`{"match_all": {}}`.

### scroll_time [String]

Amount of time Elasticsearch keeps the search context alive for scroll
requests.

### scroll_size [Int]

Maximum number of hits to be returned with each Elasticsearch scroll request.

### index_list [Array]

Define a multi-index synchronization task. It is an array that contains the
parameters required for single-table synchronization, such as `query`,
`source`, `schema`, `scroll_size`, and `scroll_time`. `index_list` and a
root-level `query` should not be configured at the same time.

### search_type [String]

Query type, available values:

- `DSL` (default): Use the Domain Specific Language query.
- `SQL`: Use a SQL query.

### search_api_type [String]

Pagination API type, available values:

- `SCROLL` (default): Use the Scroll API for pagination.
- `PIT`: Use the Point-in-Time (PIT) API for pagination.

### pit_keep_alive [Long]

The amount of time (in milliseconds) the PIT should be kept alive.

### pit_batch_size [Int]

Maximum number of hits returned per PIT search request.

### tls_verify_certificate [Boolean]

Enable certificate validation for HTTPS endpoints.

### tls_verify_hostname [Boolean]

Enable hostname validation for HTTPS endpoints.

### tls_keystore_path [String]

Path to the PEM or JKS key store. Must be readable by the user running
SeaTunnel.

### tls_keystore_password [String]

Key password for `tls_keystore_path`.

### tls_truststore_path [String]

Path to the PEM or JKS trust store. Must be readable by the user running
SeaTunnel.

### tls_truststore_password [String]

Key password for `tls_truststore_path`.

### array_column [Map]

Mark fields as array columns because Elasticsearch has no array type. Example:
`{c_array = "array<tinyint>"}`.

### runtime_fields [Array]

Runtime fields computed at query time (Elasticsearch 7.11+). Each runtime
field should contain:

- **name**: Name of the runtime field.
- **type**: Data type (boolean, date, double, geo_point, ip, keyword, long).
- **script**: Painless script to compute the field value.
- **script_lang** (optional): Script language (default: painless).
- **script_params** (optional): Script parameters.

```hocon
runtime_fields = [
  {
    name = "day_of_week"
    type = "keyword"
    script = "emit(doc['timestamp'].value.dayOfWeekEnum.toString())"
  },
  {
    name = "total_price"
    type = "double"
    script = "emit(doc['quantity'].value * doc['price'].value)"
  }
]
```

Common use cases:

- Date extraction (day of week, month, year from timestamps).
- Calculations (derived values like total price, tax amount).
- String operations (concatenation, substrings).
- Conditional logic (categorize data based on conditions).
- Data transformation (unit conversions, formatting).

Performance considerations:

- Runtime fields are computed at query time and may impact performance for
  large datasets.
- Best suited for ad-hoc analysis, prototyping, and infrequent queries.
- Keep scripts simple to minimize performance impact.
- Consider indexing frequently used computed fields.

Limitations:

- Requires Elasticsearch 7.11 or higher.
- Only Painless scripts are supported.
- May be slower than indexed fields for large-scale queries.

### slice_max [Int]

Split a single index into multiple slices for parallel reads. Only effective
for `SCROLL` / `PIT`. Set to a value greater than 1 to enable slicing.

Version requirements:

- `SCROLL` slicing (sliced scroll) requires Elasticsearch 5.0 or higher.
- `PIT` slicing requires Elasticsearch 7.10 or higher (PIT was introduced
  in 7.10.0).

Trade-offs:

- Slicing improves throughput but may reduce snapshot consistency across
  slices. For strong consistency, prefer `PIT` with a shared snapshot or
  set `slice_max = 1`.
- For append-only or low-write workloads, slicing is usually acceptable.
- `slice_max` is ignored when `search_type = "SQL"`, because Elasticsearch
  SQL search does not support slicing.

### common options

Source plugin common parameters, please refer to
[Source Common Options](../common-options/source-common-options.md) for
details.

## Task Example

### Demo 1: Read from a single index pattern with column projection

> Read documents matching the `seatunnel-*` pattern. The query restricts to
> documents containing the `id`, `name`, `age`, `tags`, and `phones` fields.
> `source` selects the fields to read, and `array_column` marks `tags` and
> `phones` as array fields.

```hocon
Elasticsearch {
    hosts = ["localhost:9200"]
    index = "seatunnel-*"
    array_column = { tags = "array<string>", phones = "array<string>" }
    source = ["_id", "name", "age", "tags", "phones"]
    query = {"range": {"firstPacket": {"gte": 1669225429990, "lte": 1669225429990}}}
}
```

### Demo 2: Multi-table synchronization

> Read different data from `read_index1` and `read_index2` and write to
> `read_index1_copy` and `read_index2_copy`. For `read_index1`, `source`
> selects the fields to read and `array_column` marks `c_array` as an
> array field.

```hocon
source {
  Elasticsearch {
    hosts = ["https://elasticsearch:9200"]
    username = "elastic"
    password = "elasticsearch"
    tls_verify_certificate = false
    tls_verify_hostname = false
    index_list = [
       {
           index = "read_index1"
           query = {"range": {"c_int": {"gte": 10, "lte": 20}}}
           source = [
             c_map, c_array, c_string, c_boolean, c_tinyint, c_smallint,
             c_bigint, c_float, c_double, c_decimal, c_bytes, c_int,
             c_date, c_timestamp
           ]
           array_column = {
             c_array = "array<tinyint>"
           }
       },
       {
           index = "read_index2"
           query = {"match_all": {}}
           source = [c_int2, c_date2, c_null]
       }
    ]
  }
}

transform {
}

sink {
  Elasticsearch {
    hosts = ["https://elasticsearch:9200"]
    username = "elastic"
    password = "elasticsearch"
    tls_verify_certificate = false
    tls_verify_hostname = false
    index = "${table_name}_copy"
    index_type = "st"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

### Demo 3: SSL (Disable certificate validation)

```hocon
source {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        username = "elastic"
        password = "elasticsearch"
        tls_verify_certificate = false
    }
}
```

### Demo 4: SSL (Disable hostname validation)

```hocon
source {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        username = "elastic"
        password = "elasticsearch"
        tls_verify_hostname = false
    }
}
```

### Demo 5: SSL (Enable certificate validation)

```hocon
source {
    Elasticsearch {
        hosts = ["https://localhost:9200"]
        username = "elastic"
        password = "elasticsearch"
        tls_keystore_path = "${your elasticsearch home}/config/certs/http.p12"
        tls_keystore_password = "${your password}"
    }
}
```

### Demo 6: SQL query

> Note: SQL search does not support map and array types.

```hocon
source {
  Elasticsearch {
    hosts = ["https://elasticsearch:9200"]
    username = "elastic"
    password = "elasticsearch"
    tls_verify_certificate = false
    tls_verify_hostname = false
    index = "st_index_sql"
    sql_query = "select * from st_index_sql where c_int>=10 and c_int<=20"
    search_type = "sql"
  }
}
```

### Demo 7: PIT

```hocon
source {
  Elasticsearch {
    hosts = ["https://elasticsearch:9200"]
    username = "elastic"
    password = "elasticsearch"
    tls_verify_certificate = false
    tls_verify_hostname = false
    index = "st_index"
    query = {"range": {"c_int": {"gte": 10, "lte": 20}}}
    search_type = DSL
    search_api_type = PIT
    pit_keep_alive = 60000
    pit_batch_size = 100
  }
}
```

### Demo 8: Runtime Fields (Elasticsearch 7.11+)

> Compute values at query time without reindexing data.

```hocon
source {
  Elasticsearch {
    hosts = ["https://elasticsearch:9200"]
    username = "elastic"
    password = "elasticsearch"
    tls_verify_certificate = false
    tls_verify_hostname = false

    index = "sales_data"

    runtime_fields = [
      {
        name = "total_amount"
        type = "double"
        script = "emit(doc['quantity'].value * doc['price'].value)"
      },
      {
        name = "day_of_week"
        type = "keyword"
        script = "emit(doc['order_date'].value.dayOfWeekEnum.getDisplayName(TextStyle.FULL, Locale.ROOT))"
      },
      {
        name = "order_category"
        type = "keyword"
        script = """
          double amount = doc['quantity'].value * doc['price'].value;
          if (amount > 1000) {
            emit('high_value');
          } else if (amount > 100) {
            emit('medium_value');
          } else {
            emit('low_value');
          }
        """
      },
      {
        name = "price_with_tax"
        type = "double"
        script = "emit(doc['price'].value * (1 + params.tax_rate))"
        script_params = {
          tax_rate = 0.13
        }
      }
    ]

    source = [
      "product_id", "quantity", "price", "order_date",
      "total_amount", "day_of_week", "order_category", "price_with_tax"
    ]

    schema = {
      fields {
        product_id = string
        quantity = int
        price = double
        order_date = timestamp
        total_amount = double
        day_of_week = string
        order_category = string
        price_with_tax = double
      }
    }
  }
}

sink {
  Console {
  }
}
```

### Demo 9: PIT with slicing

```hocon
source {
  Elasticsearch {
    hosts = ["https://elasticsearch:9200"]
    username = "elastic"
    password = "elasticsearch"
    tls_verify_certificate = false
    tls_verify_hostname = false
    index = "st_index"
    query = {"range": {"c_int": {"gte": 10, "lte": 20}}}
    search_type = DSL
    search_api_type = PIT
    pit_keep_alive = 60000
    pit_batch_size = 100
    slice_max = 2
  }
}
```

## Changelog

<ChangeLog />