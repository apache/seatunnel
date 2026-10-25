import ChangeLog from '../changelog/connector-kudu.md';

# Kudu

> Kudu sink connector

## Support Kudu Version

- 1.11.1/1.12.0/1.13.0/1.14.0/1.15.0

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Key Features

- [ ] [exactly-once](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [support multiple table write](../../introduction/concepts/connector-v2-features.md)
- [ ] [timer flush](../../introduction/concepts/connector-v2-features.md)

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
| table_name                                | String | No       | Upstream table name                            | The name of the Kudu table. If this option is omitted, SeaTunnel uses the upstream table name. In multi-table jobs, placeholders such as `${database_name}`, `${schema_name}`, and `${table_name}` can be used. |
| client_worker_count                       | Int    | No       | 2 * Runtime.getRuntime().availableProcessors() | Kudu worker count. Default value is twice the current number of cpu cores.                                                                  |
| client_default_operation_timeout_ms       | Long   | No       | 30000                                          | Kudu normal operation time out.                                                                                                             |
| client_default_admin_operation_timeout_ms | Long   | No       | 30000                                          | Kudu admin operation time out.                                                                                                              |
| enable_kerberos                           | Bool   | No       | false                                          | Kerberos principal enable.                                                                                                                  |
| kerberos_principal                        | String | Yes, when `enable_kerberos = true` | -                                              | Kerberos principal used by the Kudu client. The keytab must be available on every worker node.                                  |
| kerberos_keytab                           | String | Yes, when `enable_kerberos = true` | -                                              | Kerberos keytab path used by the Kudu client. The file must be available on every worker node.                                  |
| kerberos_krb5conf                         | String | No       | -                                              | Kerberos krb5 conf. Note that all zeta nodes require have this file.                                                                        |
| save_mode                                 | String | No       | APPEND                                         | Storage mode. Supported values are `append` and `overwrite`. In `overwrite` mode, insert rows are written as Kudu upserts.                                                                                             |
| session_flush_mode                        | String | No       | AUTO_FLUSH_SYNC                                | Kudu flush mode. Supported values are `AUTO_FLUSH_SYNC`, `AUTO_FLUSH_BACKGROUND`, and `MANUAL_FLUSH`.                                                                                                   |
| batch_size                                | Int    | No       | 1024                                           | Required only when `session_flush_mode` is `AUTO_FLUSH_BACKGROUND` or `MANUAL_FLUSH`. The writer flushes after this many append, upsert, or delete records. |
| buffer_flush_interval                     | Int    | No       | 10000                                          | Required only when `session_flush_mode = AUTO_FLUSH_BACKGROUND`. The asynchronous writer flush interval, in milliseconds.                                                             |
| ignore_not_found                          | Bool   | No       | false                                          | If true, ignore all not found rows.                                                                                                         |
| ignore_not_duplicate                      | Bool   | No       | false                                          | If true, ignore all duplicate rows.                                                                                                          |
| multi_table_sink_replica                  | Int    | No       | 1                                              | Number of sink writer replicas for each table in a multi-table job.                                                                          |
| common-options                            |        | No       | -                                              | Sink plugin common parameters, please refer to [Sink Common Options](../common-options/sink-common-options.md) for details.                            |

## Option Notes

- `table_name` is optional. If it is not set, the sink writes to the table name carried by the upstream row.
- For multi-table jobs, use placeholders in `table_name` to route rows to different Kudu tables.
- CDC rows are supported: insert records are appended, update records are written as upserts, and delete records are deleted by key.
- `batch_size` is required only for `AUTO_FLUSH_BACKGROUND` or `MANUAL_FLUSH`. `buffer_flush_interval`
  is required only for `AUTO_FLUSH_BACKGROUND`.
- When `enable_kerberos = true`, both `kerberos_principal` and `kerberos_keytab` are required.

## Task Example

### Simple

> The following example refers to a FakeSource named "kudu" cdc write kudu table "kudu_sink_table"

```hocon

env {
  parallelism = 1
  job.mode = "BATCH"
}
    source {
      FakeSource {
       plugin_output = "kudu"
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
    plugin_input = "kudu"
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
    url = "jdbc:mysql://127.0.0.1:3306/seatunnel"
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

<ChangeLog />
