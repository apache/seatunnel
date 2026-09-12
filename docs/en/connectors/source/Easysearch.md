import ChangeLog from '../changelog/connector-easysearch.md';

# Easysearch

> Easysearch source connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

Used to read data from INFINI Easysearch.

## Using Dependency

> Dependency [easysearch-client](https://central.sonatype.com/artifact/com.infinilabs/easysearch-client)

## Key features

- [x] [batch](../../introduction/concepts/connector-v2-features.md)
- [ ] [stream](../../introduction/concepts/connector-v2-features.md)
- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [column projection](../../introduction/concepts/connector-v2-features.md)
- [ ] [parallelism](../../introduction/concepts/connector-v2-features.md)
- [ ] [support user-defined split](../../introduction/concepts/connector-v2-features.md)
- [ ] [support multiple table read](../../introduction/concepts/connector-v2-features.md)

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

## Source Options

| name                   | type    | required | default value | description |
|------------------------|---------|----------|---------------|-------------|
| hosts                  | array   | yes      | -             | Easysearch HTTP addresses. |
| index                  | string  | yes      | -             | Easysearch index name. Wildcard matching such as `seatunnel-*` is supported. |
| username               | string  | no       | -             | Username for secured Easysearch clusters. |
| password               | string  | no       | -             | Password for secured Easysearch clusters. |
| source                 | array   | no       | -             | Fields to read from the index. Configure either `source` or `schema`. |
| schema                 | config  | no       | -             | SeaTunnel schema used to read and convert fields. Configure either `schema` or `source`. |
| query                  | json    | no       | `{"match_all":{}}` | Easysearch DSL query used to filter records. |
| scroll_time            | string  | no       | 1m            | Time that Easysearch keeps the scroll context alive. |
| scroll_size            | int     | no       | 100           | Maximum records returned by each scroll request. |
| tls_verify_certificate | boolean | no       | true          | Whether to validate HTTPS certificates. |
| tls_verify_hostname    | boolean | no       | true          | Whether to validate HTTPS host names. |
| tls_keystore_path      | string  | no       | -             | Path to the PEM or JKS key store. |
| tls_keystore_password  | string  | no       | -             | Password for the configured key store. |
| tls_truststore_path    | string  | no       | -             | Path to the PEM or JKS trust store. |
| tls_truststore_password | string | no       | -             | Password for the configured trust store. |
| common-options         | config  | no       | -             | Source plugin common options. |

### hosts [array]

Easysearch cluster http address, the format is `host:port`, allowing multiple hosts to be specified. Such as `["host1:9200", "host2:9200"]`.

### username [string]

security username.

### password [string]

security password.

### index [string]

Easysearch index name, support * fuzzy matching.

### source [array]

The fields to read from the index.

You can get the document id by specifying the field `_id`. If you need to write `_id` to another
Easysearch index, specify an alias for `_id` because Easysearch does not allow `_id` to be written
as a normal field.

`source` and `schema` are mutually exclusive. If both are omitted, the connector reads the field
mapping from Easysearch and uses all mapped fields in the index.

### query [json]

Easysearch DSL.
You can control the range of data read.

### scroll_time [String]

Amount of time Easysearch will keep the search context alive for scroll requests.

### scroll_size [int]

Maximum number of hits to be returned with each Easysearch scroll request.

### schema

The structure of the data, including field names and field types. For more details, see
[Schema Feature](../../introduction/concepts/schema-feature.md).

`schema` and `source` are mutually exclusive. Use `schema` when you want SeaTunnel to convert the
selected fields with an explicit SeaTunnel type definition. If both are omitted, the connector reads
the field mapping from Easysearch and uses all mapped fields in the index.

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

Source plugin common parameters, please refer to [Source Common Options](../common-options/source-common-options.md) for details.

## Examples

### Read Selected Fields

```hocon
source {
  Easysearch {
    hosts = ["localhost:9200"]
    index = "seatunnel-*"
    source = ["_id", "name", "age"]
    query = {"range": {"age": {"gte": 18, "lte": 60}}}
  }
}
```

### Read With Schema And Query

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Easysearch {
    hosts = ["https://e2e_easysearch:9200"]
    username = "admin"
    password = "admin"
    tls_verify_certificate = false
    tls_verify_hostname = false

    index = "st_index"
    query = {"range": {"c_int": {"gte": 10, "lte": 20}}}
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
  }
}

sink {
  Easysearch {
    hosts = ["https://e2e_easysearch:9200"]
    username = "admin"
    password = "admin"
    tls_verify_certificate = false
    tls_verify_hostname = false

    index = "st_index2"
  }
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

<ChangeLog />
