# Kudu

> Kudu水槽连接器

## 支持Kudu版本

- 1.11.1/1.12.0/1.13.0/1.14.0/1.15.0

## 支持以下引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [exactly-once](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)
- [x] [support multiple table write](../../concept/connector-v2-features.md)

## 数据类型映射

| SeaTunnel 数据类型 |      Kudu 数据类型      |
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

## Sink 选项

|                   名称                    |  类型  | 需要 |                    默认                     |                                                                 描述                                                                 |
|-------------------------------------------|--------|----------|------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| kudu_masters                              | String | 是      | -                                              | Kudu master address. Separated by ',',such as '192.168.88.110:7051'.                                                                        |
| table_name                                | String | 是      | -                                              | The name of kudu table.                                                                                                                     |
| client_worker_count                       | Int    | 否       | 2 * Runtime.getRuntime().availableProcessors() | Kudu worker count. Default value is twice the current number of cpu cores.                                                                  |
| client_default_operation_timeout_ms       | Long   | 否       | 30000                                          | Kudu normal operation time out.                                                                                                             |
| client_default_admin_operation_timeout_ms | Long   | 否       | 30000                                          | Kudu admin operation time out.                                                                                                              |
| enable_kerberos                           | Bool   | 否       | false                                          | Kerberos principal enable.                                                                                                                  |
| kerberos_principal                        | String | 否       | -                                              | Kerberos principal. Note that all zeta nodes require have this file.                                                                        |
| kerberos_keytab                           | String | 否       | -                                              | Kerberos keytab. Note that all zeta nodes require have this file.                                                                           |
| kerberos_krb5conf                         | String | 否       | -                                              | Kerberos krb5 conf. Note that all zeta nodes require have this file.                                                                        |
| save_mode                                 | String | 否       | -                                              | Storage mode, support `overwrite` and `append`.                                                                                             |
| session_flush_mode                        | String | 否       | AUTO_FLUSH_SYNC                                | Kudu flush mode. Default AUTO_FLUSH_SYNC.                                                                                                   |
| batch_size                                | Int    | 否       | 1024                                           | The flush max size (includes all append, upsert and delete records), over this number of records, will flush data. The default value is 100 |
| buffer_flush_interval                     | Int    | 否       | 10000                                          | The flush interval mills, over this time, asynchronous threads will flush data.                                                             |
| ignore_not_found                          | Bool   | 否       | false                                          | If true, ignore all not found rows.                                                                                                         |
| ignore_not_duplicate                      | Bool   | 否       | false                                          | If true, ignore all dulicate rows.                                                                                                          |
| common-options                            |        | 否       | -                                              | Source plugin common parameters, please refer to [Source Common Options](../sink-common-options.md) for details.                            |

## Task Example

### 简单的:

> 以下示例引用了一个名为“kudu”的FakeSource cdc-write-kudu表“kudu_sink_table”

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

#### 示例1

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

#### 示例2

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

- 添加Kudu水槽连接器

### 2.3.0-beta 2022-10-20

- [改进]Kudu Sink连接器支持追加销售行（[2881](https://github.com/apache/seatunnel/pull/2881))

### 下一个版本

- 将插件名称从“KuduSink”更改为“Kudu”[3432](https://github.com/apache/seatunnel/pull/3432)

