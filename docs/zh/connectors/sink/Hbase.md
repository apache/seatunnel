import ChangeLog from '../changelog/connector-hbase.md';

# Hbase

> Hbase 接收器连接器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

将 SeaTunnel 数据写入 Apache HBase。支持创建或复用目标表、写入单表或多表、把字段映射到一个或多个列簇，并可控制空值、行键、WAL、时间戳和已有数据的处理方式。

## 主要特性

- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表写入](../../introduction/concepts/connector-v2-features.md)
- [ ] [定时刷新](../../introduction/concepts/connector-v2-features.md)

## 选项

| 名称                     | 类型    | 是否必须 | 默认值                         | 描述 |
|--------------------------|---------|----------|--------------------------------|------|
| zookeeper_quorum         | string  | 是       | -                              | HBase ZooKeeper 地址，例如 `hadoop001:2181,hadoop002:2181`。 |
| table                    | string  | 是       | -                              | 目标 HBase 表。多表写入时可使用 `${table_name}` 等占位符。 |
| rowkey_column            | list    | 是       | -                              | 用来组成 HBase rowkey 的上游字段名。 |
| family_name              | config  | 是       | -                              | 上游字段到 HBase 列簇的映射，也可以用 `all_columns` 统一写入一个列簇。 |
| rowkey_delimiter         | string  | 否       | ""                             | 多个字段组成 rowkey 时使用的连接符。 |
| version_column           | string  | 否       | -                              | 用作 HBase 单元格时间戳的上游字段。 |
| null_mode                | string  | 否       | skip                           | null 值写入方式，支持 `skip` 和 `empty`。 |
| wal_write                | boolean | 否       | false                          | 写入时是否记录 HBase WAL。 |
| write_buffer_size        | int     | 否       | 8 * 1024 * 1024                | HBase 客户端写入缓冲区大小，单位字节。 |
| encoding                 | string  | 否       | utf8                           | STRING/DECIMAL/DATE/TIME/TIMESTAMP/ARRAY 的编码，支持 `utf8` 和 `gbk`。 |
| schema_save_mode         | enum    | 否       | CREATE_SCHEMA_WHEN_NOT_EXIST   | 写入前如何处理目标表结构。 |
| data_save_mode           | enum    | 否       | APPEND_DATA                    | 写入前如何处理目标端已有数据。 |
| hbase_extra_config       | config  | 否       | -                              | 额外的 HBase 或 Hadoop 客户端配置。 |
| multi_table_sink_replica | int     | 否       | 1                              | 多表写入时每张表对应的 Sink Writer 副本数。 |
| common-options           |         | 否       | -                              | Sink 插件通用参数，例如 `plugin_input`。 |

### zookeeper_quorum [string]

HBase 的 zookeeper 集群主机，示例: "hadoop001:2181,hadoop002:2181,hadoop003:2181"

### table [string]

要写入的表名, 例如: "seatunnel"
如果表在自定义 namespace 下，请使用 `namespace:table` 形式（如 `ns1:seatunnel_test`）；未填写 namespace 时，SeaTunnel 会写入到 HBase 默认命名空间 `default`。

### rowkey_column [list]

行键的列名列表, 例如: ["id", "uuid"]

### family_name [config]

字段的列簇名称映射。例如,上游的行如下所示：

| id |     name      | age |
|----|---------------|-----|
| 1  | tyrantlucifer | 27  |

`id` 作为行键，其他字段可以写入不同列簇。例如:

family_name {
name = "info1"
age = "info2"
}

这表示 `name` 写入列簇 `info1`，`age` 写入列簇 `info2`。

如果要将其他字段写入同一列簇，可以分配

family_name {
all_columns = "info"
}

这表示所有字段都会写入列簇 `info`。

### rowkey_delimiter [string]

连接多行键的分隔符，默认 ""

### version_column [string]

版本列名称，您可以使用它来分配 hbase 记录的时间戳

### null_mode [string]

写入 null 值的模式，支持 [ skip , empty], 默认 skip

- skip: 当字段为 null ,连接器不会将此字段写入 hbase
- empty: 当字段为 null 时，连接器会为此字段写入空值

### wal_write [boolean]

wal log 写入标志，默认值 false

### write_buffer_size [int]

hbase 客户端的写入缓冲区大小，默认 8 * 1024 * 1024

### encoding [string]

字符串类字段的编码（STRING/DECIMAL/DATE/TIME/TIMESTAMP/ARRAY），支持 [utf8, gbk]，默认 utf8

### 数据类型

Hbase 存储字节，连接器支持：

- TINYINT/SMALLINT/INT/BIGINT/FLOAT/DOUBLE/BOOLEAN/BYTES
- STRING/DECIMAL/DATE/TIME/TIMESTAMP/ARRAY（使用 encoding 序列化为字符串后写入）

### hbase_extra_config [config]

hbase 扩展配置

### multi_table_sink_replica [int]

多表写入时，每张表对应的 Sink Writer 副本数。通常保持默认值即可；只有单表写入压力较大、需要更多写入并行度时再调大。

### schema_save_mode [enum]

写入前如何处理目标 HBase 表结构。常用值包括 `RECREATE_SCHEMA`、`CREATE_SCHEMA_WHEN_NOT_EXIST` 和
`ERROR_WHEN_SCHEMA_NOT_EXIST`。

### data_save_mode [enum]

写入前如何处理目标端已有数据。支持 `DROP_DATA`、`APPEND_DATA` 和 `ERROR_WHEN_DATA_EXISTS`。

### 常见选项

Sink 插件常用参数，详见 Sink 常用选项 [Sink Common Options](../common-options/sink-common-options.md)

## 注意事项

- `table = "${table_name}"` 会把每张上游表写入同名 HBase 表，也可以和固定文本组合，例如 `ods_${table_name}`。
- `family_name { all_columns = "info" }` 会把所有非 rowkey 字段写入同一个列簇。不同字段需要写入不同列簇时，请使用逐字段映射。
- `schema_save_mode` 控制是否创建或重建表，`data_save_mode` 控制保留、清空已有数据，还是在已有数据存在时报错。

## 案例

### 写入单表

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

sink {
  Hbase {
    zookeeper_quorum = "hadoop001:2181,hadoop002:2181,hadoop003:2181"
    table = "seatunnel_test"
    rowkey_column = ["name"]
    family_name {
      all_columns = "seatunnel"
    }
  }
}
```

### 目标表不存在时自动创建

```hocon
sink {
  Hbase {
    zookeeper_quorum = "hbase_e2e:2181"
    table = "seatunnel_test_with_create_when_not_exists"
    rowkey_column = ["name"]
    family_name {
      all_columns = info
    }
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode = "APPEND_DATA"
  }
}
```

### 每次写入前重建目标表

当任务允许在每次运行时删除并重建目标表时，可以使用 `schema_save_mode = "RECREATE_SCHEMA"`。
配合 `data_save_mode = "DROP_DATA"` 可以同时清空已有数据；如果只想刷新表结构，保留
`APPEND_DATA` 即可。

```hocon
sink {
  Hbase {
    zookeeper_quorum = "hbase_e2e:2181"
    table = "seatunnel_test_with_recreate_schema"
    rowkey_column = ["name"]
    family_name {
      all_columns = info
    }
    schema_save_mode = "RECREATE_SCHEMA"
  }
}
```

## Kerberos 示例

备注：

- `connector-hbase` 不会解析 `krb5_path` / `kerberos_principal` / `kerberos_keytab_path`。
- 需要在运行环境中提前完成 Kerberos 登录并保证 `krb5.conf` 可被 JVM 访问（例如 `kinit -kt ...` 或 JVM `-Djava.security.krb5.conf=...`），同时将 HBase/Hadoop 的安全配置写入 `hbase_extra_config`。

```hocon
sink {
  Hbase {
    zookeeper_quorum = "zk1:2181,zk2:2181,zk3:2181"
    table = "target_table"
    rowkey_column = ["rowkey"]
    family_name {
      all_columns = "info"
    }

    # HBase安全配置
    hbase_extra_config = {
      "hbase.security.authentication" = "kerberos"
      "hadoop.security.authentication" = "kerberos"
      "hbase.master.kerberos.principal" = "hbase/_HOST@REALM"
      "hbase.regionserver.kerberos.principal" = "hbase/_HOST@REALM"
      "hbase.rpc.protection" = "authentication"
      "hbase.zookeeper.useSasl" = "false"
    }
  }
}
```

### 写入多表

```hocon
env {
  # You can set engine configuration here
  execution.parallelism = 1
  job.mode = "BATCH"
}

source {
  FakeSource {
    tables_configs = [
      {
        schema = {
          table = "hbase_sink_1"
          fields {
            name = STRING
            c_string = STRING
            c_double = DOUBLE
            c_bigint = BIGINT
            c_float = FLOAT
            c_int = INT
            c_smallint = SMALLINT
            c_boolean = BOOLEAN
            time = BIGINT
          }
        }
        rows = [
          {
            kind = INSERT
            fields = ["label_1", "sink_1", 4.3, 200, 2.5, 2, 5, true, 1627529632356]
          }
        ]
      },
      {
        schema = {
          table = "hbase_sink_2"
          fields {
            name = STRING
            c_string = STRING
            c_double = DOUBLE
            c_bigint = BIGINT
            c_float = FLOAT
            c_int = INT
            c_smallint = SMALLINT
            c_boolean = BOOLEAN
            time = BIGINT
          }
        }
        rows = [
          {
            kind = INSERT
            fields = ["label_2", "sink_2", 4.3, 200, 2.5, 2, 5, true, 1627529632357]
          }
        ]
      }
    ]
  }
}

sink {
  Hbase {
    zookeeper_quorum = "hadoop001:2181,hadoop002:2181,hadoop003:2181"
    table = "${table_name}"
    rowkey_column = ["name"]
    family_name {
      all_columns = "info"
    }
  }
}
```

## 写入指定列族

```hocon
Hbase {
  zookeeper_quorum = "hbase_e2e:2181"
  table = "assign_cf_table"
  rowkey_column = ["id"]
  family_name {
    c_double = "cf1"
    c_bigint = "cf2"
  }
}
```

## 变更日志

<ChangeLog />
