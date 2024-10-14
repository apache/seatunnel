# Elasticsearch

> Elasticsearch source connector

## Description

Used to read data from Elasticsearch.

support version >= 2.x and <= 8.x.

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Options

| name                    | type    | required | default value                                                  |
|-------------------------|---------|----------|----------------------------------------------------------------|
| hosts                   | array   | yes      | -                                                              |
| username                | string  | no       | -                                                              |
| password                | string  | no       | -                                                              |
| index                   | string  | no       | If the index list does not exist, the index must be configured |
| index_list              | array   | no       | used to define a multiple table task                           |
| source                  | array   | no       | -                                                              |
| query                   | json    | no       | {"match_all": {}}                                              |
| scroll_time             | string  | no       | 1m                                                             |
| scroll_size             | int     | no       | 100                                                            |
| tls_verify_certificate  | boolean | no       | true                                                           |
| tls_verify_hostnames    | boolean | no       | true                                                           |
| array_column            | map     | no       |                                                                |
| tls_keystore_path       | string  | no       | -                                                              |
| tls_keystore_password   | string  | no       | -                                                              |
| tls_truststore_path     | string  | no       | -                                                              |
| tls_truststore_password | string  | no       | -                                                              |
| common-options          |         | no       | -                                                              |



### hosts [array]

Elasticsearch cluster http address, the format is `host:port`, allowing multiple hosts to be specified. Such as `["host1:9200", "host2:9200"]`.

### username [string]

x-pack username.

### password [string]

x-pack password.

### index [string]

Elasticsearch index name, support * fuzzy matching.

### source [array]

The fields of index.
You can get the document id by specifying the field `_id`.If sink _id to other index,you need specify an alias for _id due to the Elasticsearch limit.
If you don't config source, it is automatically retrieved from the mapping of the index.

### array_column [array]

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

### common options

Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details

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

## Changelog

### next version

- Add Elasticsearch Source Connector
- [Feature] Support https protocol & compatible with opensearch ([3997](https://github.com/apache/seatunnel/pull/3997))
- [Feature] Support DSL