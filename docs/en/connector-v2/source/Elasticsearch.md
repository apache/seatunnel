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

|          name           |  type   | required |   default value   |
|-------------------------|---------|----------|-------------------|
| hosts                   | array   | yes      | -                 |
| username                | string  | no       | -                 |
| password                | string  | no       | -                 |
| index                   | string  | yes      | -                 |
| source                  | array   | no       | -                 |
| query                   | json    | no       | {"match_all": {}} |
| scroll_time             | string  | no       | 1m                |
| scroll_size             | int     | no       | 100               |
| schema                  |         | no       | -                 |
| tls_verify_certificate  | boolean | no       | true              |
| tls_verify_hostnames    | boolean | no       | true              |
| tls_keystore_path       | string  | no       | -                 |
| tls_keystore_password   | string  | no       | -                 |
| tls_truststore_path     | string  | no       | -                 |
| tls_truststore_password | string  | no       | -                 |
| common-options          |         | no       | -                 |

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
If you don't config source, you must config `schema`.

### query [json]

Elasticsearch DSL.
You can control the range of data read.

### scroll_time [String]

Amount of time Elasticsearch will keep the search context alive for scroll requests.

### scroll_size [int]

Maximum number of hits to be returned with each Elasticsearch scroll request.

### schema

The structure of the data, including field names and field types.
If you don't config schema, you must config `source`.

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

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details

## Examples

simple

```hocon
Elasticsearch {
    hosts = ["localhost:9200"]
    index = "seatunnel-*"
    source = ["_id","name","age"]
    query = {"range":{"firstPacket":{"gte":1669225429990,"lte":1669225429990}}}
}
```

complex

```hocon
Elasticsearch {
    hosts = ["elasticsearch:9200"]
    index = "st_index"
    schema = {
        fields {
            c_map = "map<string, tinyint>"
            c_array = "array<tinyint>"
            c_string = string
            c_boolean = boolean
            c_tinyint = tinyint
            c_smallint = smallint
            c_int = int
            c_bigint = bigint
            c_float = float
            c_double = double
            c_decimal = "decimal(2, 1)"
            c_bytes = bytes
            c_date = date
            c_timestamp = timestamp
        }
    }
    query = {"range":{"firstPacket":{"gte":1669225429990,"lte":1669225429990}}}
}
```

SSL (Disable certificates validation)

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

SSL (Disable hostname validation)

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

SSL (Enable certificates validation)

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

