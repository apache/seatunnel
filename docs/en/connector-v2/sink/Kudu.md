# Kudu

> Kudu sink connector

## Support Kudu Version

- 1.11.1/1.12.0/1.13.0/1.14.0/1.15.0

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)
- [x] [support multiple table write](../../concept/connector-v2-features.md)

## Data Type Mapping

| SeaTunnel Data Type |      Kudu Data Type      |
|---------------------|--------------------------|
| BOOLEAN             | BOOL                     |
| INT                 | INT8<br/>INT16<br/>INT32 |
| BIGINT              | INT64                    |
| DECIMAL             | DECIMAL                  |
| FLOAT               | FLOAT                    |
| DOUBLE              | DOUBLE                   |
| STRING              | STRING                   |
| TIMESTAMP           | UNIXTIME_MICROS          |
| BYTES               | BINARY                   |

## Sink Options

|                   Name                    |  Type  | Required |                    Default                     |                                                                 Description                                                                 |
|-------------------------------------------|--------|----------|------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| kudu_masters                              | String | Yes      | -                                              | Kudu master address. Separated by ',',such as '192.168.88.110:7051'.                                                                        |
| table_name                                | String | Yes      | -                                              | The name of kudu table.                                                                                                                     |
| client_worker_count                       | Int    | No       | 2 * Runtime.getRuntime().availableProcessors() | Kudu worker count. Default value is twice the current number of cpu cores.                                                                  |
| client_default_operation_timeout_ms       | Long   | No       | 30000                                          | Kudu normal operation time out.                                                                                                             |
| client_default_admin_operation_timeout_ms | Long   | No       | 30000                                          | Kudu admin operation time out.                                                                                                              |
| enable_kerberos                           | Bool   | No       | false                                          | Kerberos principal enable.                                                                                                                  |
| kerberos_principal                        | String | No       | -                                              | Kerberos principal. Note that all zeta nodes require have this file.                                                                        |
| kerberos_keytab                           | String | No       | -                                              | Kerberos keytab. Note that all zeta nodes require have this file.                                                                           |
| kerberos_krb5conf                         | String | No       | -                                              | Kerberos krb5 conf. Note that all zeta nodes require have this file.                                                                        |
| save_mode                                 | String | No       | -                                              | Storage mode, support `overwrite` and `append`.                                                                                             |
| session_flush_mode                        | String | No       | AUTO_FLUSH_SYNC                                | Kudu flush mode. Default AUTO_FLUSH_SYNC.                                                                                                   |
| batch_size                                | Int    | No       | 1024                                           | The flush max size (includes all append, upsert and delete records), over this number of records, will flush data. The default value is 100 |
| buffer_flush_interval                     | Int    | No       | 10000                                          | The flush interval mills, over this time, asynchronous threads will flush data.                                                             |
| ignore_not_found                          | Bool   | No       | false                                          | If true, ignore all not found rows.                                                                                                         |
| ignore_not_duplicate                      | Bool   | No       | false                                          | If true, ignore all dulicate rows.                                                                                                          |
| common-options                            |        | No       | -                                              | Source plugin common parameters, please refer to [Source Common Options](../sink-common-options.md) for details.                            |

## Task Example

### Simple:

> The following example refers to a FakeSource named "kudu" cdc write kudu table "kudu_sink_table"

```hocon

env {
  parallelism = 1
  job.mode = "BATCH"
}
    source {
      FakeSource {
       result_table_name = "kudu"
        schema = {
          fields {
                    id = int
                    val_bool = boolean
                    val_int8 = tinyint
                    val_int16 = smallint
                    val_int32 = int
                    val_int64 = bigint
                    val_float = float
                    val_double = double
                    val_decimal = "decimal(16, 1)"
                    val_string = string
                    val_unixtime_micros = timestamp
          }
        }
        rows = [
          {
            kind = INSERT
            fields = [1, true, 1, 2, 3, 4, 4.3,5.3,6.3, "NEW", "2020-02-02T02:02:02"]
          },
          {
            kind = INSERT
            fields = [2, true, 1, 2, 3, 4, 4.3,5.3,6.3, "NEW", "2020-02-02T02:02:02"]
          },
          {
            kind = INSERT
            fields = [3, true, 1, 2, 3, 4, 4.3,5.3,6.3, "NEW", "2020-02-02T02:02:02"]
          },
          {
            kind = UPDATE_BEFORE
            fields = [1, true, 1, 2, 3, 4, 4.3,5.3,6.3, "NEW", "2020-02-02T02:02:02"]
          },
          {
            kind = UPDATE_AFTER
           fields = [1, true, 2, 2, 3, 4, 4.3,5.3,6.3, "NEW", "2020-02-02T02:02:02"]
          },
          {
            kind = DELETE
            fields = [2, true, 1, 2, 3, 4, 4.3,5.3,6.3, "NEW", "2020-02-02T02:02:02"]
          }
        ]
      }
    }

sink {
   kudu{
    source_table_name = "kudu"
    kudu_masters = "kudu-master-cdc:7051"
    table_name = "kudu_sink_table"
    enable_kerberos = true
    kerberos_principal = "xx@xx.COM"
    kerberos_keytab = "xx.keytab"
 }
}
```

### Multiple table

#### example1

```hocon
env {
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  Mysql-CDC {
    base-url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
    username = "root"
    password = "******"
    
    table-names = ["seatunnel.role","seatunnel.user","galileo.Bucket"]
  }
}

transform {
}

sink {
  kudu{
    kudu_masters = "kudu-master-cdc:7051"
    table_name = "${database_name}_${table_name}_test"
  }
}
```

#### example2

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    driver = oracle.jdbc.driver.OracleDriver
    url = "jdbc:oracle:thin:@localhost:1521/XE"
    user = testUser
    password = testPassword

    table_list = [
      {
        table_path = "TESTSCHEMA.TABLE_1"
      },
      {
        table_path = "TESTSCHEMA.TABLE_2"
      }
    ]
  }
}

transform {
}

sink {
  kudu{
    kudu_masters = "kudu-master-cdc:7051"
    table_name = "${schema_name}_${table_name}_test"
  }
}
```

## Changelog

### 2.2.0-beta 2022-09-26

- Add Kudu Sink Connector

### 2.3.0-beta 2022-10-20

- [Improve] Kudu Sink Connector Support to upsert row ([2881](https://github.com/apache/seatunnel/pull/2881))

### Next Version

- Change plugin name from `KuduSink` to `Kudu` [3432](https://github.com/apache/seatunnel/pull/3432)

