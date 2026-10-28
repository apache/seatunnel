import ChangeLog from '../changelog/connector-elasticsearch.md';

# Elasticsearch

> Elasticsearch source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Used to read data from Elasticsearch.

support version >= 2.x and <= 8.x.

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [x] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

## Options

| name                    | type    | required | default value                                                  | description |
|-------------------------|---------|----------|----------------------------------------------------------------|-------------|
| hosts                   | array   | yes      | -                                                              | Elasticsearch cluster HTTP addresses in `host:port` form. Multiple hosts can be specified (for example `["host1:9200", "host2:9200"]`). |
| auth_type               | string  | no       | basic                                                          | Authentication method. Supported values: `basic`, `api_key`, `api_key_encoded`. |
| username                | string  | no       | -                                                              | Username for basic authentication (x-pack username). Required when `auth_type=basic`. |
| password                | string  | no       | -                                                              | Password for basic authentication (x-pack password). Required when `auth_type=basic`. |
| auth.api_key_id         | string  | no       | -                                                              | Elasticsearch API key ID. Required when `auth_type=api_key`. |
| auth.api_key            | string  | no       | -                                                              | Elasticsearch API key secret. Required when `auth_type=api_key`. |
| auth.api_key_encoded    | string  | no       | -                                                              | Base64 encoded API key (`base64(id:api_key)`). Required when `auth_type=api_key_encoded`. |
| index                   | string  | no       | Required when `index_list` is not configured                    | Single Elasticsearch index or index pattern. Supports `*` fuzzy matching. |
| index_list              | array   | no       | Used to define a multi-index task                              | List of indexes to read; each entry can override `query`, `source`, `schema`, `scroll_size`, and `scroll_time`. |
| source                  | array   | no       | -                                                              | Document fields to project. Use the `_id` alias to read the document id. If unset, fields are auto-retrieved from the index mapping. |
| query                   | json    | no       | `{"match_all": {}}`                                            | Elasticsearch DSL body. Controls which documents are read. |
| search_type             | enum    | no       | DSL                                                            | Query type: `DSL` (default) or `SQL`. |
| search_api_type         | enum    | no       | SCROLL                                                         | Pagination API: `SCROLL` (default) or `PIT`. |
| sql_query               | string  | no       | Required when `search_type=SQL`                                | SQL query used when `search_type=SQL`. Map and array types are not supported. |
| scroll_time             | string  | no       | 1m                                                             | Time the Elasticsearch scroll context stays alive. |
| scroll_size             | int     | no       | 100                                                            | Maximum number of hits returned per scroll request. |
| tls_verify_certificate  | boolean | no       | true                                                           | Enable certificate validation for HTTPS endpoints. |
| tls_verify_hostname     | boolean | no       | true                                                           | Enable hostname validation for HTTPS endpoints. |
| array_column            | map     | no       | -                                                              | Map of column names to array element types (for example `c_array = "array<tinyint>"`). |
| tls_keystore_path       | string  | no       | -                                                              | Path to the PEM or JKS key store. Must be readable by the user running SeaTunnel. |
| tls_keystore_password   | string  | no       | -                                                              | Password for the key store specified by `tls_keystore_path`. |
| tls_truststore_path     | string  | no       | -                                                              | Path to the PEM or JKS trust store. Must be readable by the user running SeaTunnel. |
| tls_truststore_password | string  | no       | -                                                              | Password for the trust store specified by `tls_truststore_path`. |
| pit_keep_alive          | long    | no       | 60000 (1 minute)                                               | PIT retention time in milliseconds. Only effective when `search_api_type=PIT`. |
| pit_batch_size          | int     | no       | 100                                                            | Maximum number of hits returned per PIT search request. |
| runtime_fields          | array   | no       | -                                                              | Runtime fields to compute at query time (Elasticsearch 7.11+). Each entry needs at least `name`, `type`, and `script`. |
| slice_max               | int     | no       | 1                                                              | Number of slices used to split a single index for parallel reads. Effective for SCROLL/PIT only. SCROLL slicing requires ES >= 5.0; PIT slicing requires ES >= 7.10. Ignored when `search_type=SQL`. |
| common-options          |         | no       | -                                                              | Source plugin common parameters; see [Source Common Options](../common-options/source-common-options.md). |



### hosts [array]

Elasticsearch cluster http address, the format is `host:port`, allowing multiple hosts to be specified. Such as `["host1:9200", "host2:9200"]`.

## Authentication

The Elasticsearch connector supports multiple authentication methods to connect to secured Elasticsearch clusters. You can choose the appropriate authentication method based on your Elasticsearch security configuration.

### auth_type [enum]

Specifies the authentication method to use. Supported values:
- `basic` (default): HTTP Basic Authentication using username and password
- `api_key`: Elasticsearch API Key authentication using separate ID and key
- `api_key_encoded`: Elasticsearch API Key authentication using encoded key

If not specified, defaults to `basic` for backward compatibility.

### Basic Authentication

Basic authentication uses HTTP Basic Authentication with username and password credentials.

#### username [string]

Username for basic authentication (x-pack username).

#### password [string]

Password for basic authentication (x-pack password).

**Example:**
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

API Key authentication provides a more secure way to authenticate with Elasticsearch using API keys.

#### auth.api_key_id [string]

The API key ID generated by Elasticsearch.

#### auth.api_key [string]

The API key secret generated by Elasticsearch.

#### auth.api_key_encoded [string]

Base64 encoded API key in the format `base64(id:api_key)`. This is an alternative to specifying `auth.api_key_id` and `auth.api_key` separately.

**Note:** You can use either `auth.api_key_id` + `auth.api_key` OR `auth.api_key_encoded`, but not both.

**Example with separate ID and key:**
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

**Example with encoded key:**
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



### index [string]

Elasticsearch index name, support * fuzzy matching.

Either `index` or `index_list` must be configured. Use `index` for a single index or an index pattern, and use `index_list` when different indexes need their own `query`, `source`, `schema`, or paging options.

### source [array]

The fields of index.
You can get the document id by specifying the field `_id`.If sink _id to other index,you need specify an alias for _id due to the Elasticsearch limit.
If you don't config source, it is automatically retrieved from the mapping of the index.

### array_column [map]

The fields of array type.
Since there is no array index in es,so need assign array type,just like `{c_array = "array<tinyint>"}`.

### query [json]

Elasticsearch DSL.
You can control the range of data read.

### scroll_time [String]

Amount of time Elasticsearch will keep the search context alive for scroll requests.

### scroll_size [int]

Maximum number of hits to be returned with each Elasticsearch scroll request.

### index_list [array]

The `index_list` is used to define multi-index synchronization tasks. It is an array that contains the parameters required for single-table synchronization, such as `query`, `source/schema`, `scroll_size`, and `scroll_time`. It is recommended that `index_list` and `query` should not be configured at the same level simultaneously. Please refer to the upcoming multi-table synchronization example for more details.

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

### search_type
Query type, available values:
- DSL: Use Domain Specific Language query (default)
- SQL: Use SQL query

### search_api_type
Pagination API type, available values:
- SCROLL: Use Scroll API for pagination (default)
- PIT: Use Point in Time (PIT) API for pagination

### pit_keep_alive [long]
The amount of time (in milliseconds) for which the PIT should be keep alive

### pit_batch_size  [int]
Maximum number of hits to be returned with each PIT search request

### runtime_fields [array]

Runtime fields to be computed at query time (Elasticsearch 7.11+). Each runtime field should contain:
- **name**: The name of the runtime field
- **type**: The data type (boolean, date, double, geo_point, ip, keyword, long)
- **script**: Painless script to compute the field value
- **script_lang** (optional): Script language (default: painless)
- **script_params** (optional): Script parameters

Example:
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

**Runtime Fields Use Cases:**

1. **Date Extraction**: Extract day of week, month, year from timestamps
2. **Calculations**: Compute derived values like total price, tax amount
3. **String Operations**: Concatenate fields, extract substrings
4. **Conditional Logic**: Categorize data based on conditions
5. **Data Transformation**: Convert units, format values on-the-fly

**Performance Considerations:**
- Runtime fields are computed at query time, which may impact performance for large datasets
- Best suited for ad-hoc analysis, prototyping, and infrequent queries
- Keep scripts simple to minimize performance impact
- Consider indexing frequently used computed fields

**Limitations:**
- Requires Elasticsearch 7.11 or higher
- Only Painless scripts are supported
- May be slower than indexed fields for large-scale queries

### slice_max [int]
Split a single index into multiple slices for parallel reads. Only effective for SCROLL/PIT. Set to a value greater than 1 to enable slicing.

**Version requirements:**
- SCROLL slicing (sliced scroll) requires Elasticsearch 5.0 or higher.
- PIT slicing requires Elasticsearch 7.10 or higher (PIT was introduced in 7.10.0).

**Trade-off:** slicing improves throughput but may reduce snapshot consistency across slices. For strong consistency, prefer PIT with a shared snapshot or set `slice_max = 1`. For append-only or low-write workloads, slicing is usually acceptable.

`slice_max` is ignored when `search_type = "SQL"` because Elasticsearch SQL search does not support slicing.

### common options

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details

## Examples

Demo 1

> This case will read data from indices matching the seatunnel-* pattern based on a query. The query will only return documents containing the id, name, age, tags, and phones fields. In this example, the source field configuration is used to specify which fields should be read, and the array_column is used to indicate that tags and phones should be treated as arrays.

```hocon
Elasticsearch {
    hosts = ["localhost:9200"]
    index = "seatunnel-*"
    array_column = {tags = "array<string>",phones = "array<string>"}
    source = ["_id","name","age","tags","phones"]
    query = {"range":{"firstPacket":{"gte":1669225429990,"lte":1669225429990}}}
}
```

Demo 2 : Multi-table synchronization

> This example demonstrates how to read different data from ``read_index1`` and ``read_index2`` and write separately to ``read_index1_copy``,``read_index2_copy``.
> in `read_index1`,I used source to specify the fields to be read and  specify which fields are array fields using the 'array_column'.

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
           c_map,
           c_array,
           c_string,
           c_boolean,
           c_tinyint,
           c_smallint,
           c_bigint,
           c_float,
           c_double,
           c_decimal,
           c_bytes,
           c_int,
           c_date,
           c_timestamp]
           array_column = {
           c_array = "array<tinyint>"
           }
       }
       {
           index = "read_index2"
           query = {"match_all": {}}
           source = [
           c_int2,
           c_date2,
           c_null
           ]

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
    "schema_save_mode"="CREATE_SCHEMA_WHEN_NOT_EXIST"
    "data_save_mode"="APPEND_DATA"
  }
}
```



Demo 3 : SSL (Disable certificates validation)

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

Demo 4 :SSL (Disable hostname validation)

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

Demo 5 :SSL (Enable certificates validation)

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

Demo 6 : sql query
notes: sql does not support map and array types
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

Demo7:  PIT
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

    # Use DSL query with PIT API
    search_type = DSL
    search_api_type = PIT
    pit_keep_alive = 60000  # 1 minute in milliseconds
    pit_batch_size = 100
  }
}
```

Demo 8: Runtime Fields (Elasticsearch 7.11+)

> This example demonstrates how to use runtime fields to compute values at query time without reindexing data.

```hocon
source {
  Elasticsearch {
    hosts = ["https://elasticsearch:9200"]
    username = "elastic"
    password = "elasticsearch"
    tls_verify_certificate = false
    tls_verify_hostname = false
    
    index = "sales_data"
    
    # Define runtime fields for dynamic computation
    runtime_fields = [
      {
        # Calculate total amount
        name = "total_amount"
        type = "double"
        script = "emit(doc['quantity'].value * doc['price'].value)"
      },
      {
        # Extract day of week from timestamp
        name = "day_of_week"
        type = "keyword"
        script = "emit(doc['order_date'].value.dayOfWeekEnum.getDisplayName(TextStyle.FULL, Locale.ROOT))"
      },
      {
        # Categorize orders
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
        # Calculate with parameters
        name = "price_with_tax"
        type = "double"
        script = "emit(doc['price'].value * (1 + params.tax_rate))"
        script_params = {
          tax_rate = 0.13
        }
      }
    ]
    
    # Include runtime fields in the output
    source = [
      "product_id",
      "quantity",
      "price",
      "order_date",
      "total_amount",
      "day_of_week",
      "order_category",
      "price_with_tax"
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

Demo 9: PIT with slicing
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

    # Enable slicing for parallel reads
    slice_max = 2
  }
}
```

## Changelog

<ChangeLog />
