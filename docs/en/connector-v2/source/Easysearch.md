# Easysearch

> Easysearch source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Used to read data from INFINI Easysearch.

## Using Dependency

> Depenndency [easysearch-client](https://central.sonatype.com/artifact/com.infinilabs/easysearch-client)

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [ ] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

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
| Date                        | LOCAL_DATE_TIME_TYPE |

### hosts [array]

Easysearch cluster http address, the format is `host:port`, allowing multiple hosts to be specified. Such as `["host1:9200", "host2:9200"]`.

### username [string]

security username.

### password [string]

security password.

### index [string]

Easysearch index name, support * fuzzy matching.

### source [array]

The fields of index.
You can get the document id by specifying the field `_id`.If sink _id to other index,you need specify an alias for _id due to the Easysearch limit.
If you don't config source, you must config `schema`.

### query [json]

Easysearch DSL.
You can control the range of data read.

### scroll_time [String]

Amount of time Easysearch will keep the search context alive for scroll requests.

### scroll_size [int]

Maximum number of hits to be returned with each Easysearch scroll request.

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

Source plugin common parameters, please refer to [Source Common Options](../source-common-options.md) for details

## Examples

simple

```hocon
Easysearch {
    hosts = ["localhost:9200"]
    index = "seatunnel-*"
    source = ["_id","name","age"]
    query = {"range":{"firstPacket":{"gte":1700407367588,"lte":1700407367588}}}
}
```

complex

```hocon
Easysearch {
    hosts = ["Easysearch:9200"]
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
    query = {"range":{"firstPacket":{"gte":1700407367588,"lte":1700407367588}}}
}
```

SSL (Disable certificates validation)

```hocon
source {
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
source {
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
source {
    Easysearch {
        hosts = ["https://localhost:9200"]
        username = "admin"
        password = "admin"
        
        tls_keystore_path = "${your Easysearch home}/config/certs/http.p12"
        tls_keystore_password = "${your password}"
    }
}
```

## Changelog

### next version

- Add Easysearch Source Connector
- Support https protocol
- Support DSL

