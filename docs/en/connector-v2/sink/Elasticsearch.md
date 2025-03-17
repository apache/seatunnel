# Elasticsearch

## Description

Output data to `Elasticsearch`.

## Key features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

:::tip

Engine Supported

* supported  `ElasticSearch version is >= 2.x and <= 8.x`

:::

## Options

|          name           |  type   | required |        default value         |
|-------------------------|---------|----------|------------------------------|
| hosts                   | array   | yes      | -                            |
| index                   | string  | yes      | -                            |
| schema_save_mode        | string  | yes      | CREATE_SCHEMA_WHEN_NOT_EXIST |
| data_save_mode          | string  | yes      | APPEND_DATA                  |
| index_type              | string  | no       |                              |
| primary_keys            | list    | no       |                              |
| key_delimiter           | string  | no       | `_`                          |
| username                | string  | no       |                              |
| password                | string  | no       |                              |
| max_retry_count         | int     | no       | 3                            |
| max_batch_size          | int     | no       | 10                           |
| tls_verify_certificate  | boolean | no       | true                         |
| tls_verify_hostnames    | boolean | no       | true                         |
| tls_keystore_path       | string  | no       | -                            |
| tls_keystore_password   | string  | no       | -                            |
| tls_truststore_path     | string  | no       | -                            |
| tls_truststore_password | string  | no       | -                            |
| common-options          |         | no       | -                            |

### hosts [array]

`Elasticsearch` cluster http address, the format is `host:port` , allowing multiple hosts to be specified. Such as `["host1:9200", "host2:9200"]`.

### index [string]

`Elasticsearch`  `index` name.Index support contains variables of field name,such as `seatunnel_${age}`(Need to configure schema_save_mode="IGNORE"),and the field must appear at seatunnel row.
If not, we will treat it as a normal index.

### index_type [string]

`Elasticsearch` index type, it is recommended not to specify in elasticsearch 6 and above

### primary_keys [list]

Primary key fields used to generate the document `_id`, this is cdc required options.

### key_delimiter [string]

Delimiter for composite keys ("_" by default), e.g., "$" would result in document `_id` "KEY1$KEY2$KEY3".

### username [string]

x-pack username

### password [string]

x-pack password

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

### schema_save_mode

Before the synchronous task is turned on, different treatment schemes are selected for the existing surface structure of the target side.
Option introduction：  
`RECREATE_SCHEMA` ：Will create when the table does not exist, delete and rebuild when the table is saved  
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：Will Created when the table does not exist, skipped when the table is saved  
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：Error will be reported when the table does not exist  
`IGNORE` ：Ignore the treatment of the table

### data_save_mode

Before the synchronous task is turned on, different processing schemes are selected for data existing data on the target side.
Option introduction：  
`DROP_DATA`： Preserve database structure and delete data  
`APPEND_DATA`：Preserve database structure, preserve data  
`ERROR_WHEN_DATA_EXISTS`：When there is data, an error is reported

## Examples

Simple

```conf
sink {
    Elasticsearch {
        hosts = ["localhost:9200"]
        index = "seatunnel-${age}"
        schema_save_mode="IGNORE"
    }
}

```
Multi-table writing

```conf
sink {
    Elasticsearch {
        hosts = ["localhost:9200"]
        index = "${table_name}"
        schema_save_mode="IGNORE"
    }
}
```

CDC(Change data capture) event

```conf
sink {
    Elasticsearch {
        hosts = ["localhost:9200"]
        index = "seatunnel-${age}"
        schema_save_mode="IGNORE"
        # cdc required options
        primary_keys = ["key1", "key2", ...]
    }
}

```
CDC(Change data capture) event Multi-table writing

```conf
sink {
    Elasticsearch {
        hosts = ["localhost:9200"]
        index = "${table_name}"
        schema_save_mode="IGNORE"
        primary_keys = ["${primary_key}"]
    }
}
```

SSL (Disable certificates validation)

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

SSL (Disable hostname validation)

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

SSL (Enable certificates validation)

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

SAVE_MODE (Add saveMode function)

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

### Schema Evolution

CDC collection supports a limited number of schema changes. The currently supported schema changes include:

* Adding columns.

### Schema Evolution
```hocon
env {
  # You can set engine configuration here
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
    base-url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
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
    "schema_save_mode" = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    "data_save_mode" = "APPEND_DATA"
  }
}
```
