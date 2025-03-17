# Doris

> Doris sink 连接器

## 支持的doris版本

- exactly-once & cdc 支持  `Doris version is >= 1.1.x`
- 支持数组数据类型 `Doris version is >= 1.2.x`
- 将支持Map数据类型 `Doris version is 2.x`

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

## 描述

用于发送数据到doris. 同时支持流模式和批模式处理.
Doris Sink连接器的内部实现是通过stream load批量缓存和导入的。

## 依赖

### 对于 Spark/Flink

> 1. 你需要下载 [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 并添加到目录 `${SEATUNNEL_HOME}/plugins/`.

### 对于 SeaTunnel Zeta

> 1. 你需要下载 [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 并添加到目录 `${SEATUNNEL_HOME}/lib/`.

## Sink 选项

|              Name              |  Type   | Required |           Default            |                                                                      Description                                                                       |
|--------------------------------|---------|----------|------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|
| fenodes                        | String  | Yes      | -                            | `Doris` 集群 fenodes 地址, 格式是 `"fe_ip:fe_http_port, ..."`                                                                                                 |
| query-port                     | int     | No       | 9030                         | `Doris` Fenodes mysql协议查询端口                                                                                                                            |
| username                       | String  | Yes      | -                            | `Doris` 用户名                                                                                                                                            |
| password                       | String  | Yes      | -                            | `Doris` 密码                                                                                                                                             |
| database                       | String  | Yes      | -                            | `Doris`数据库名称 , 使用 `${database_name}` 表示上游数据库名称。                                                                                                        |
| table                          | String  | Yes      | -                            | `Doris` 表名,  使用 `${table_name}`  表示上游表名。                                                                                                               |
| table.identifier               | String  | Yes      | -                            | `Doris` 表的名称，2.3.5 版本后将弃用，请使用 `database` 和 `table` 代替。                                                                                                 |
| sink.label-prefix              | String  | Yes      | -                            | stream load导入使用的标签前缀。 在2pc场景下，需要全局唯一性来保证SeaTunnel的EOS语义。                                                                                               |
| sink.enable-2pc                | bool    | No       | false                        | 是否启用两阶段提交（2pc），默认为 false。 对于两阶段提交，请参考[此处](https://doris.apache.org/docs/data-operate/transaction?_highlight=two&_highlight=phase#stream-load-2pc)。 |
| sink.enable-delete             | bool    | No       | -                            | 是否启用删除。 该选项需要Doris表开启批量删除功能（0.15+版本默认开启），且仅支持Unique模型。 您可以在此[link](https://doris.apache.org/docs/dev/data-operate/delete/batch-delete-manual/)获得更多详细信息 |
| sink.check-interval            | int     | No       | 10000                        | 加载过程中检查异常时间间隔。                                                                                                                                        |
| sink.max-retries               | int     | No       | 3                            | 向数据库写入记录失败时的最大重试次数。                                                                                                                                   |
| sink.buffer-size               | int     | No       | 256 * 1024                   | 用于缓存stream load数据的缓冲区大小。                                                                                                                              |
| sink.buffer-count              | int     | No       | 3                            | 用于缓存stream load数据的缓冲区计数。                                                                                                                              |
| doris.batch.size               | int     | No       | 1024                         | 每次http请求写入doris的批量大小，当row达到该大小或者执行checkpoint时，缓存的数据就会写入服务器。                                                                                           |
| needs_unsupported_type_casting | boolean | No       | false                        | 是否启用不支持的类型转换，例如 Decimal64 到 Double。                                                                                                                   |
| schema_save_mode               | Enum    | no       | CREATE_SCHEMA_WHEN_NOT_EXIST | schema保存模式，请参考下面的`schema_save_mode`                                                                                                                   |
| data_save_mode                 | Enum    | no       | APPEND_DATA                  | 数据保存模式，请参考下面的`data_save_mode`。                                                                                                                        |
| save_mode_create_template      | string  | no       | see below                    | 见下文。                                                                                                                                                  |
| custom_sql                     | String  | no       | -                            | 当data_save_mode选择CUSTOM_PROCESSING时，需要填写CUSTOM_SQL参数。 该参数通常填写一条可以执行的SQL。 SQL将在同步任务之前执行。                                                               |
| doris.config                   | map     | yes      | -                            | 该选项用于支持自动生成sql时的insert、delete、update等操作，以及支持的格式。                                                                                                      |

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
并从“rowtype_fields”中删除 id 字段。 该方法可用于自定义字段类型和属性的修改。

可以使用以下占位符：

- database：用于获取上游schema中的数据库。
- table_name：用于获取上游schema中的表名。
- rowtype_fields：用于获取上游schema中的所有字段，自动映射到Doris的字段描述。
- rowtype_primary_key：用于获取上游模式中的主键（可能是列表）。
- rowtype_unique_key：用于获取上游模式中的唯一键（可能是列表）。
- comment：用于获取上游模式中的表注释。

## 数据类型映射

|   Doris 数据类型   |             SeaTunnel 数据类型              |
|----------------|-----------------------------------------|
| BOOLEAN        | BOOLEAN                                 |
| TINYINT        | TINYINT                                 |
| SMALLINT       | SMALLINT<br/>TINYINT                    |
| INT            | INT<br/>SMALLINT<br/>TINYINT            |
| BIGINT         | BIGINT<br/>INT<br/>SMALLINT<br/>TINYINT |
| LARGEINT       | BIGINT<br/>INT<br/>SMALLINT<br/>TINYINT |
| FLOAT          | FLOAT                                   |
| DOUBLE         | DOUBLE<br/>FLOAT                        |
| DECIMAL        | DECIMAL<br/>DOUBLE<br/>FLOAT            |
| DATE           | DATE                                    |
| DATETIME       | TIMESTAMP                               |
| CHAR           | STRING                                  |
| VARCHAR        | STRING                                  |
| STRING         | STRING                                  |
| ARRAY          | ARRAY                                   |
| MAP            | MAP                                     |
| JSON           | STRING                                  |
| HLL            | 尚不支持                                    |
| BITMAP         | 尚不支持                                    |
| QUANTILE_STATE | 尚不支持                                    |
| STRUCT         | 尚不支持                                    |

#### 支持的导入数据格式

支持的格式包括 CSV 和 JSON。

## 调优指南
适当增加`sink.buffer-size`和`doris.batch.size`的值可以提高写性能。

在流模式下，如果`doris.batch.size`和`checkpoint.interval`都配置为较大的值，最后到达的数据可能会有较大的延迟(延迟的时间就是检查点间隔的时间)。

这是因为最后到达的数据总量可能不会超过doris.batch.size指定的阈值。因此，在接收到数据的数据量没有超过该阈值之前只有检查点才会触发提交操作。因此，需要选择一个合适的检查点间隔。

此外，如果你通过`sink.enable-2pc=true`属性启用2pc。`sink.buffer-size`将会失去作用，只有检查点才能触发提交。

## 任务示例

### 简单示例:

> 下面的例子描述了向Doris写入多种数据类型，用户需要在下游创建对应的表。

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
  Doris {
    fenodes = "doris_cdc_e2e:8030"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    sink.label-prefix = "test-cdc"
    sink.enable-2pc = "true"
    sink.enable-delete = "true"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}
```

### CDC（监听数据变更捕获）事件：

> 本示例定义了一个SeaTunnel同步任务，通过FakeSource自动生成数据并发送给Doris Sink，FakeSource使用schema、score（int类型）模拟CDC数据，Doris需要创建一个名为test.e2e_table_sink的sink任务及其对应的表 。

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
  Doris {
    fenodes = "doris_cdc_e2e:8030"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    sink.label-prefix = "test-cdc"
    sink.enable-2pc = "true"
    sink.enable-delete = "true"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}

```

### 使用JSON格式导入数据

```
sink {
    Doris {
        fenodes = "e2e_dorisdb:8030"
        username = root
        password = ""
        database = "test"
        table = "e2e_table_sink"
        sink.enable-2pc = "true"
        sink.label-prefix = "test_json"
        doris.config = {
            format="json"
            read_json_by_line="true"
        }
    }
}

```

### 使用CSV格式导入数据

```
sink {
    Doris {
        fenodes = "e2e_dorisdb:8030"
        username = root
        password = ""
        database = "test"
        table = "e2e_table_sink"
        sink.enable-2pc = "true"
        sink.label-prefix = "test_csv"
        doris.config = {
          format = "csv"
          column_separator = ","
        }
    }
}
```

## 变更日志

### 2.3.0-beta 2022-10-20

- 添加 Doris sink连接器

### Next version

- [Improve] Change Doris Config Prefix [3856](https://github.com/apache/seatunnel/pull/3856)

- [Improve] Refactor some Doris Sink code as well as support 2pc and cdc [4235](https://github.com/apache/seatunnel/pull/4235)

:::tip

PR 4235 is an incompatible modification to PR 3856. Please refer to PR 4235 to use the new Doris connector

:::
