import ChangeLog from '../changelog/connector-kudu.md';

# Kudu

> Kudu Sink 连接器

## 支持 Kudu 版本

- 1.11.1/1.12.0/1.13.0/1.14.0/1.15.0

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [变更数据捕获](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

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
| table_name                                | String | 否      | 上游表名                                      | Kudu 表名。不配置时，SeaTunnel 使用上游数据携带的表名。多表写入时可使用 `${database_name}`、`${schema_name}`、`${table_name}` 等占位符。 |
| client_worker_count                       | Int    | 否       | 2 * Runtime.getRuntime().availableProcessors() | Kudu工人数。默认值是当前cpu核数的两倍。                                                                  |
| client_default_operation_timeout_ms       | Long   | 否       | 30000                                          | Kudu正常运行超时。                                                                                                             |
| client_default_admin_operation_timeout_ms | Long   | 否       | 30000                                          | Kudu管理员操作超时。                                                                                                              |
| enable_kerberos                           | Bool   | 否       | false                                          | 启用Kerberos主体。                                                                                                                  |
| kerberos_principal                        | String | 否       | -                                              | Kerberos主体。请注意，所有zeta节点都需要此文件。                                                                        |
| kerberos_keytab                           | String | 否       | -                                              | Kerberos密钥表。请注意，所有zeta节点都需要此文件。                                                                           |
| kerberos_krb5conf                         | String | 否       | -                                              | Kerberos krb5 conf.请注意，所有zeta节点都需要此文件。                                                                        |
| save_mode                                 | String | 否       | APPEND                                         | 存储模式。支持 `append` 和 `overwrite`。在 `overwrite` 模式下，插入记录会按 Kudu upsert 写入。                                                                                             |
| session_flush_mode                        | String | 否       | AUTO_FLUSH_SYNC                                | Kudu 刷新模式。支持 `AUTO_FLUSH_SYNC`、`AUTO_FLUSH_BACKGROUND` 和 `MANUAL_FLUSH`。                                                                                                   |
| batch_size                                | Int    | 否       | 1024                                           | 仅当 `session_flush_mode` 为 `AUTO_FLUSH_BACKGROUND` 或 `MANUAL_FLUSH` 时需要关注。写入的 append、upsert、delete 记录达到该数量后会刷新。 |
| buffer_flush_interval                     | Int    | 否       | 10000                                          | 仅当 `session_flush_mode = AUTO_FLUSH_BACKGROUND` 时生效，表示异步写入的刷新间隔，单位毫秒。                                                             |
| ignore_not_found                          | Bool   | 否       | false                                          | 如果为true，则忽略所有未找到的行。                                                                                                         |
| ignore_not_duplicate                      | Bool   | 否       | false                                          | 如果为 true，则忽略所有重复行。                                                                                                          |
| common-options                            |        | 否       | -                                              | Sink插件常用参数，详见[Sink common Options](../common-options/sink-common-options.md)。                           |

## 参数说明

- `table_name` 是可选参数。不配置时，Sink 会写入上游数据行携带的表名。
- 多表写入时，可以在 `table_name` 中使用占位符，把不同来源表的数据写入不同 Kudu 表。
- 支持 CDC 数据：插入记录会追加写入，更新记录会按 upsert 写入，删除记录会按主键删除。

## 任务示例

### 简单示例

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
