import ChangeLog from '../changelog/connector-kudu.md';

# Kudu

> Kudu数据接收器

## 支持Kudu版本

- 1.11.1/1.12.0/1.13.0/1.14.0/1.15.0

## 支持引擎

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

|                   名称                    |  类型  | 是否必填 |                    默认值                     |                                                                 描述                                                                 |
|-------------------------------------------|--------|----------|------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| kudu_masters                              | String | 是      | -                                              | Kudu主地址。用“，”分隔，例如“192.168.88.110:7051”。                                                                        |
| table_name                                | String | 是      | -                                              | Kudu表的名字。                                                                                                                     |
| client_worker_count                       | Int    | 否       | 2 * Runtime.getRuntime().availableProcessors() | Kudu工人数。默认值是当前cpu核数的两倍。                                                                  |
| client_default_operation_timeout_ms       | Long   | 否       | 30000                                          | Kudu正常运行超时。                                                                                                             |
| client_default_admin_operation_timeout_ms | Long   | 否       | 30000                                          | Kudu管理员操作超时。                                                                                                              |
| enable_kerberos                           | Bool   | 否       | false                                          | 启用Kerberos主体。                                                                                                                  |
| kerberos_principal                        | String | 否       | -                                              | Kerberos主体。请注意，所有zeta节点都需要此文件。                                                                        |
| kerberos_keytab                           | String | 否       | -                                              | Kerberos密钥表。请注意，所有zeta节点都需要此文件。                                                                           |
| kerberos_krb5conf                         | String | 否       | -                                              | Kerberos krb5 conf.请注意，所有zeta节点都需要此文件。                                                                        |
| save_mode                                 | String | 否       | -                                              | 存储模式，支持 `overwrite` 和 `append`.                                                                                             |
| session_flush_mode                        | String | 否       | AUTO_FLUSH_SYNC                                | Kudu刷新模式。默认AUTO_FLUSH_SYNC。                                                                                                   |
| batch_size                                | Int    | 否       | 1024                                           | 超过此记录数的刷新最大大小（包括所有追加、追加和删除记录）将刷新数据。默认值为100 |
| buffer_flush_interval                     | Int    | 否       | 10000                                          | 刷新间隔期间，异步线程将刷新数据。                                                             |
| ignore_not_found                          | Bool   | 否       | false                                          | 如果为true，则忽略所有未找到的行。                                                                                                         |
| ignore_not_duplicate                      | Bool   | 否       | false                                          | 如果为true，则忽略所有dulicate行。                                                                                                          |
| common-options                            |        | 否       | -                                              |源插件常用参数，详见[Source common Options]（../sink common-Options.md）。                           |

## 任务示例

### 简单示例:

> 以下示例引用了FakeSource kudu写入表kudu_sink_table

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

### 多表

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

## 变更日志

<ChangeLog />
