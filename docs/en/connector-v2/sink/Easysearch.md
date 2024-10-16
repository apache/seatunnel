# INFINI Easysearch

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Description

A sink plugin which use send data to `INFINI Easysearch`.

## Using Dependency

> Depenndency [easysearch-client](https://central.sonatype.com/artifact/com.infinilabs/easysearch-client)
>
  ## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

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

## Sink Options

|          name           |  type   | required | default value |
|-------------------------|---------|----------|---------------|
| hosts                   | array   | yes      | -             |
| index                   | string  | yes      | -             |
| primary_keys            | list    | no       |               |
| key_delimiter           | string  | no       | `_`           |
| username                | string  | no       |               |
| password                | string  | no       |               |
| max_retry_count         | int     | no       | 3             |
| max_batch_size          | int     | no       | 10            |
| tls_verify_certificate  | boolean | no       | true          |
| tls_verify_hostnames    | boolean | no       | true          |
| tls_keystore_path       | string  | no       | -             |
| tls_keystore_password   | string  | no       | -             |
| tls_truststore_path     | string  | no       | -             |
| tls_truststore_password | string  | no       | -             |
| common-options          |         | no       | -             |

### hosts [array]

`INFINI Easysearch` cluster http address, the format is `host:port` , allowing multiple hosts to be specified. Such as `["host1:9200", "host2:9200"]`.

### index [string]

`INFINI Easysearch`  `index` name.Index support contains variables of field name,such as `seatunnel_${age}`,and the field must appear at seatunnel row.
If not, we will treat it as a normal index.

### primary_keys [list]

Primary key fields used to generate the document `_id`, this is cdc required options.

### key_delimiter [string]

Delimiter for composite keys ("_" by default), e.g., "$" would result in document `_id` "KEY1$KEY2$KEY3".

### username [string]

security username

### password [string]

security password

### max_retry_count [int]

one bulk request max try size

### max_batch_size [int]

batch bulk doc max size

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

Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details

## Examples

Simple

```bash
sink {
    Easysearch {
        hosts = ["localhost:9200"]
        index = "seatunnel-${age}"
    }
}
```

CDC(Change data capture) event

```bash
sink {
    Easysearch {
        hosts = ["localhost:9200"]
        index = "seatunnel-${age}"
        
        # cdc required options
        primary_keys = ["key1", "key2", ...]
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

## Changelog

### 2.3.4 2023-11-16

- Add Easysearch Sink Connector
- Support http/https protocol
- Support CDC write DELETE/UPDATE/INSERT events

