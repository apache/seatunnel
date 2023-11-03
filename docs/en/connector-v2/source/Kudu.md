# Kudu

> Kudu source connector

## Support Kudu Version

- 1.11.1/1.12.0/1.13.0/1.14.0/1.15.0

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [ ] [support user-defined split](../../concept/connector-v2-features.md)

## Description

Used to read data from Kudu.

The tested kudu version is 1.11.1.

## Data Type Mapping

|      kudu Data type      | SeaTunnel Data type |
|--------------------------|---------------------|
| BOOL                     | BOOLEAN             |
| INT8<br/>INT16<br/>INT32 | INT                 |
| INT64                    | BIGINT              |
| DECIMAL                  | DECIMAL             |
| FLOAT                    | FLOAT               |
| DOUBLE                   | DOUBLE              |
| STRING                   | STRING              |
| UNIXTIME_MICROS          | TIMESTAMP           |
| BINARY                   | BYTES               |

## Source Options

|                   Name                    |  Type  | Required |                    Default                     |                                               Description                                                |
|-------------------------------------------|--------|----------|------------------------------------------------|----------------------------------------------------------------------------------------------------------|
| kudu_masters                              | String | Yes      | -                                              | Kudu master address. Separated by ',',such as '192.168.88.110:7051'.                                     |
| table_name                                | String | Yes      | -                                              | The name of kudu table.                                                                                  |
| client_worker_count                       | Int    | No       | 2 * Runtime.getRuntime().availableProcessors() | Kudu worker count. Default value is twice the current number of cpu cores.                               |
| client_default_operation_timeout_ms       | Long   | No       | 30000                                          | Kudu normal operation time out.                                                                          |
| client_default_admin_operation_timeout_ms | Long   | No       | 30000                                          | Kudu admin operation time out.                                                                           |
| enable_kerberos                           | Bool   | No       | false                                          | Kerberos principal enable.                                                                               |
| kerberos_principal                        | String | No       | -                                              | Kerberos principal. Note that all zeta nodes require have this file.                                     |
| kerberos_keytab                           | String | No       | -                                              | Kerberos keytab. Note that all zeta nodes require have this file.                                        |
| kerberos_krb5conf                         | String | No       | -                                              | Kerberos krb5 conf. Note that all zeta nodes require have this file.                                     |
| scan_token_query_timeout                  | Long   | No       | 30000                                          | The timeout for connecting scan token. If not set, it will be the same as operationTimeout.              |
| scan_token_batch_size_bytes               | Int    | No       | 1024 * 1024                                    | Kudu scan bytes. The maximum number of bytes read at a time, the default is 1MB.                         |
| filter                                    | Int    | No       | 1024 * 1024                                    | Kudu scan filter expressions,Not supported yet.                                                          |
| schema                                    | Map    | No       | 1024 * 1024                                    | SeaTunnel Schema.                                                                                        |
| common-options                            |        | No       | -                                              | Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details. |

## Task Example

### Simple:

> The following example is for a Kudu table named "kudu_source_table", The goal is to print the data from this table on the console and write kudu table "kudu_sink_table"

```hocon
# Defining the runtime environment
env {
  # You can set flink configuration here
  execution.parallelism = 2
  job.mode = "BATCH"
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  kudu{
   kudu_masters = "kudu-master:7051"
   table_name = "kudu_source_table"
   result_table_name = "kudu"
   enable_kerberos = true
   kerberos_principal = "xx@xx.COM"
   kerberos_keytab = "xx.keytab"
}
}

transform {
}

sink {
  console {
    source_table_name = "kudu"
  }

   kudu{
    source_table_name = "kudu"
    kudu_masters = "kudu-master:7051"
    table_name = "kudu_sink_table"
    enable_kerberos = true
    kerberos_principal = "xx@xx.COM"
    kerberos_keytab = "xx.keytab"
 }
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Kudu Source Connector

### Next Version

- Change plugin name from `KuduSource` to `Kudu` [3432](https://github.com/apache/seatunnel/pull/3432)

