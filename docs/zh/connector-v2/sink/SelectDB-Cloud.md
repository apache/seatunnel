import ChangeLog from '../changelog/connector-selectdb-cloud.md';

# SelectDB Cloud

> SelectDB Cloud Sink 连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

## 描述

用于发送数据到SelectDB Cloud. 同时支持流模式和批模式处理. SelectDB Cloud Sink连接器的内部实现是通过stream load批量缓存和导入的。

## 支持的数据源信息

:::提示

支持的版本

* 支持的 `SelectDB Cloud 版本 >= 2.2.x`

:::

## 接收器选项

| 名称                             | 类型      | 是否必填 | 默认值                          | 描述                                                                                                                                               |
|--------------------------------|---------|----------|------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| fenodes                        | String  | 是       | -                            | `SelectDB Cloud` 集群fenodes地址，格式`"fe_ip:fe_http_port, ..."`                                                                                       |
| query-port                     | String  | 是       | 9030                         | `SelectDB Cloud` Fenodes mysql协议查询端口                                                                                                             |
| cluster-name                   | String  | 是       | -                            | `SelectDB Cloud` 集群名称                                                                                                                            |
| username                       | String  | 是       | -                            | `SelectDB Cloud` 用户名                                                                                                                             |
| password                       | String  | 是       | -                            | `SelectDB Cloud` 用户密码                                                                                                                            |
| database                       | String  | 是       | -                            | `SelectDB Cloud` 数据库名称，使用`${database_name}`表示上游数据库名称                                                                                             |
| table                          | String  | 是       | -                            | `SelectDB Cloud` 表名，使用`${table_name}`表示上游表名。                                                                                                     |
| sink.label-prefix              | String  | 是       | -                            | `SelectDB Cloud` stream load导入使用的标签前缀。在2pc场景下，需要全局唯一性来保证Seatunnel的EOS语义                                                                          |
| sink.enable-2pc                | bool    | 否       | false                        | 是否启用两阶段提交（2pc），默认为 false，对于两阶段提交，请参考[此处](https://doris.apache.org/docs/data-operate/transaction?_highlight=two&_highlight=phase#stream-load-2pc) |
| sink.enable-delete             | bool    | 否       | false                        | 是否启用删除功能。此选项要求 SelectDB Cloud 表启用批量删除功能，并且仅支持 Unique 模型。                                                                                         |
| sink.check-interval            | int     | 否       | 10000                        | 加载过程中检查异常时间间隔。                                                                                                                                   |
| sink.max-retries               | int     | 否       | 3                            | 写入数据库失败时的最大重试次数                                                                                                                                  |
| sink.buffer-size               | int     | 否       | 256 * 1024                   | 用于缓存stream load数据的缓冲区大小。                                                                                                                         |
| sink.buffer-count              | int     | 否       | 3                            | 用于缓存stream load数据的缓冲区计数。                                                                                                                         |
| selectdb.batch.size            | int     | 否       | 1024                         | 每次http请求写入selectdb的批量大小，当row达到该大小或者执行checkpoint时，缓存的数据就会写入服务器。                                                                                   |
| needs_unsupported_type_casting | boolean | 否       | false                        | 是否启用不支持的类型转换，例如 Decimal64 到 Double。                                                                                                              |
| case_sensitive                 | boolean | 否       | true                         | 是否保留表名和字段名的原始大小写。当设置为 false 时，表名和字段名将被转换为小写。                                                                                                     |
| schema_save_mode               | Enum    | 否       | CREATE_SCHEMA_WHEN_NOT_EXIST | schema保存模式，请参考下面的schema_save_mode                                                                                                                |
| data_save_mode                 | Enum    | 否       | APPEND_DATA                  | 数据保存模式，请参考下面的data_save_mode。                                                                                                                     |
| save_mode_create_template      | string  | 否       | see below                    | 见下文。                                                                                                                                             |
| custom_sql                     | string  | 否       | -                            | 当data_save_mode选择CUSTOM_PROCESSING时，需要填写CUSTOM_SQL参数。 该参数通常填写一条可以执行的SQL。 SQL将在同步任务之前执行。                                                                                                                                            |
| selectdb.config                | map     | 是       | -                            | 此选项用于在自动生成 SQL 时支持 `insert`、`delete` 和 `update` 等操作，并支持多种格式。                                                                                     |

### schema_save_mode[Enum]

在开启同步任务之前，针对现有的表结构选择不同的处理方案。
选项介绍：  
`RECREATE_SCHEMA` ：表不存在时创建，表保存时删除并重建。
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：表不存在时会创建，表存在时跳过。  
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：表不存在时会报错。  
`IGNORE` ：忽略对表的处理。

### data_save_mode[Enum]

在开启同步任务之前，针对目标端已有的数据选择不同的处理方案。
选项介绍：  
`DROP_DATA`： 保留数据库结构并删除数据。  
`APPEND_DATA`：保留数据库结构，保留数据。  
`CUSTOM_PROCESSING`：用户自定义处理。  
`ERROR_WHEN_DATA_EXISTS`：有数据时报错。

### save_mode_create_template

使用模板自动创建Doris表，
会根据上游数据类型和schema类型创建相应的建表语句，
默认模板可以根据情况进行修改。

默认模板：

```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table_name}` (
${rowtype_primary_key},
${rowtype_fields}
) ENGINE=OLAP
 UNIQUE KEY (${rowtype_primary_key})
COMMENT '${comment}'
DISTRIBUTED BY HASH (${rowtype_primary_key})
 PROPERTIES (
"replication_allocation" = "tag.location.default: 1",
"in_memory" = "false",
"storage_format" = "V2",
"disable_auto_compaction" = "false"
)
```

如果模板中填写了自定义字段，例如添加 id 字段

```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table_name}`
(   
    id,
    ${rowtype_fields}
) ENGINE = OLAP UNIQUE KEY (${rowtype_primary_key})
    COMMENT '${comment}'
    DISTRIBUTED BY HASH (${rowtype_primary_key})
    PROPERTIES
(
    "replication_num" = "1"
);
```

连接器会自动从上游获取对应类型完成填充，
并从"rowtype_fields"中删除 id 字段。 该方法可用于自定义字段类型和属性的修改。

可以使用以下占位符：

- database：用于获取上游schema中的数据库。
- table_name：用于获取上游schema中的表名。
- rowtype_fields：用于获取上游schema中的所有字段，自动映射到Doris的字段描述。
- rowtype_primary_key：用于获取上游模式中的主键（可能是列表）。
- rowtype_unique_key：用于获取上游模式中的唯一键（可能是列表）。
- comment：用于获取上游模式中的表注释。

## 数据类型映射

| SelectDB Cloud 数据类型 |           SeaTunnel 数据类型           |
|--------------------------|-----------------------------------------|
| BOOLEAN                  | BOOLEAN                                 |
| TINYINT                  | TINYINT                                 |
| SMALLINT                 | SMALLINT<br/>TINYINT                    |
| INT                      | INT<br/>SMALLINT<br/>TINYINT            |
| BIGINT                   | BIGINT<br/>INT<br/>SMALLINT<br/>TINYINT |
| LARGEINT                 | BIGINT<br/>INT<br/>SMALLINT<br/>TINYINT |
| FLOAT                    | FLOAT                                   |
| DOUBLE                   | DOUBLE<br/>FLOAT                        |
| DECIMAL                  | DECIMAL<br/>DOUBLE<br/>FLOAT            |
| DATE                     | DATE                                    |
| DATETIME                 | TIMESTAMP                               |
| CHAR                     | STRING                                  |
| VARCHAR                  | STRING                                  |
| STRING                   | STRING                                  |
| ARRAY                    | ARRAY                                   |
| MAP                      | MAP                                     |
| JSON                     | STRING                                  |
| HLL                      | 尚未支持                                |
| BITMAP                   | 尚未支持                                |
| QUANTILE_STATE           | 尚未支持                                |
| STRUCT                   | 尚未支持                                |

#### 支持的导入数据格式

支持的格式包括 CSV 和 JSON

## 调优指南
适当增加`sink.buffer-size`和`selectdb.batch.size`的值可以提高写性能。

在流模式下，如果`selectdb.batch.size`和`checkpoint.interval`都配置为较大的值，最后到达的数据可能会有较大的延迟(延迟的时间就是检查点间隔的时间)。

这是因为最后到达的数据总量可能不会超过selectdb.batch.size指定的阈值。因此，在接收到数据的数据量没有超过该阈值之前只有检查点才会触发提交操作。因此，需要选择一个合适的检查点间隔。

此外，如果你通过`sink.enable-2pc=true`属性启用2pc。`sink.buffer-size`将会失去作用，只有检查点才能触发提交。

## 任务示例

### 简单示例

> 以下示例描述了将多种数据类型写入 SelectDBCloud，用户需要在下游创建相应的表

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval = 10000
}

source {
  FakeSource {
    row.num = 10
    map.size = 10
    array.size = 10
    bytes.length = 10
    string.length = 10
    schema = {
      fields {
        c_map = "map<string, array<int>>"
        c_array = "array<int>"
        c_string = string
        c_boolean = boolean
        c_tinyint = tinyint
        c_smallint = smallint
        c_int = int
        c_bigint = bigint
        c_float = float
        c_double = double
        c_decimal = "decimal(16, 1)"
        c_null = "null"
        c_bytes = bytes
        c_date = date
        c_timestamp = timestamp
      }
    }
    }
}

sink {
  SelectDBCloud {
    fenodes = "warehouse_ip:http_port"
    cluster-name = "Cluster"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    sink.label-prefix = "test-cdc"
    sink.enable-2pc = "true"
    sink.enable-delete = "true"
    selectdb.config {
        format = "json"
        read_json_by_line = "true"
    }
  }
}
```


### CDC（监听数据变更捕获）事件

> 本示例定义了一个SeaTunnel同步任务，通过FakeSource自动生成数据并发送给SelectDB Sink，FakeSource使用schema、score（int类型）模拟CDC数据，SelectDB需要创建一个名为test.e2e_table_sink的sink任务及其对应的表 。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
  checkpoint.interval = 10000
}

source {
  FakeSource {
    schema = {
      fields {
        pk_id = bigint
        name = string
        score = int
        sex = boolean
        number = tinyint
        height = float
        sight = double
        create_time = date
        update_time = timestamp
      }
    }
    rows = [
      {
        kind = INSERT
        fields = [1, "A", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      },
      {
        kind = INSERT
        fields = [2, "B", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      },
      {
        kind = INSERT
        fields = [3, "C", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      },
      {
        kind = UPDATE_BEFORE
        fields = [1, "A", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      },
      {
        kind = UPDATE_AFTER
        fields = [1, "A_1", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      },
      {
        kind = DELETE
        fields = [2, "B", 100, true, 1, 170.0, 4.3, "2020-02-02", "2020-02-02T02:02:02"]
      }
    ]
  }
}

sink {
  SelectDBCloud {
    fenodes = "e2e_selectdb:8030"
    cluster-name = "Cluster"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    sink.label-prefix = "test-cdc"
    sink.enable-2pc = "true"
    sink.enable-delete = "true"
    selectdb.config {
        format = "json"
        read_json_by_line = "true"
    }
  }
}

```

### 使用 JSON 格式导入数据

```
sink {
  SelectDBCloud {
    fenodes = "e2e_selectdb:8030"
    cluster-name = "Cluster"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    sink.label-prefix = "test-json"
    sink.enable-2pc = "true"
    sink.enable-delete = "true"
    selectdb.config {
        format = "json"
        read_json_by_line = "true"
    }
  }
}

```

### 使用 CSV 格式导入数据

```
sink {
  SelectDBCloud {
    fenodes = "e2e_selectdb:8030"
    cluster-name = "Cluster"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    sink.label-prefix = "test-csv"
    sink.enable-2pc = "true"
    selectdb.config {
        format = "csv"
        column_separator = ","
    }
  }
}
```

### 大小写敏感配置

```hocon
sink {
  SelectDBCloud {
    fenodes = "e2e_selectdb:8030"
    cluster-name = "Cluster"
    username = root
    password = ""
    database = "Test_DB"  # 保留原始大小写
    table = "Test_Table"  # 保留原始大小写
    case_sensitive = true # 默认值，保留原始大小写
    sink.label-prefix = "test_case_sensitive"
    sink.enable-2pc = "true"
    selectdb.config {
        format = "json"
        read_json_by_line = "true"
    }
  }
}
```

## 变更日志

<ChangeLog />