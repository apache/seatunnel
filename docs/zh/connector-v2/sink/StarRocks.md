# StarRocks

> StarRocks 数据接收器

## 引擎支持

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要特性

- [ ] [精准一次](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)

## 描述

该接收器用于将数据写入到StarRocks中。支持批和流两种模式。
StarRocks数据接收器内部实现采用了缓存，通过stream load将数据批导入。

## 依赖

### 对于 Spark/Flink

> 1. 你需要下载 [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 并添加到目录 `${SEATUNNEL_HOME}/plugins/`.

### 对于 SeaTunnel Zeta

> 1. 你需要下载 [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 并添加到目录 `${SEATUNNEL_HOME}/lib/`.

## 接收器选项

|             名称              |   类型    | 是否必须 |             默认值              |                                                     Description                                                     |
|-----------------------------|---------|------|------------------------------|---------------------------------------------------------------------------------------------------------------------|
| nodeUrls                    | list    | yes  | -                            | `StarRocks`集群地址, 格式为 `["fe_ip:fe_http_port", ...]`                                                                  |
| base-url                    | string  | yes  | -                            | JDBC URL样式的连接信息。如：`jdbc:mysql://localhost:9030/` 或 `jdbc:mysql://localhost:9030` 或 `jdbc:mysql://localhost:9030/db` |
| username                    | string  | yes  | -                            | 目标`StarRocks` 用户名                                                                                                   |
| password                    | string  | yes  | -                            | 目标`StarRocks` 密码                                                                                                    |
| database                    | string  | yes  | -                            | 指定目标 StarRocks 表所在的数据库的名称                                                                                           |
| table                       | string  | no   | -                            | 指定目标 StarRocks 表的名称, 如果没有设置该值，则表名与上游表名相同                                                                            |
| labelPrefix                 | string  | no   | -                            | StarRocks stream load作业标签前缀                                                                                         |
| batch_max_rows              | long    | no   | 1024                         | 在批写情况下，当缓冲区数量达到`batch_max_rows`数量或`batch_max_bytes`字节大小或者时间达到`checkpoint.interval`时，数据会被刷新到StarRocks                |
| batch_max_bytes             | int     | no   | 5 * 1024 * 1024              | 在批写情况下，当缓冲区数量达到`batch_max_rows`数量或`batch_max_bytes`字节大小或者时间达到`checkpoint.interval`时，数据会被刷新到StarRocks                |
| max_retries                 | int     | no   | -                            | 数据写入StarRocks失败后的重试次数                                                                                               |
| retry_backoff_multiplier_ms | int     | no   | -                            | 用作生成下一个退避延迟的乘数                                                                                                      |
| max_retry_backoff_ms        | int     | no   | -                            | 向StarRocks发送重试请求之前的等待时长                                                                                             |
| enable_upsert_delete        | boolean | no   | false                        | 是否开启upsert/delete事件的同步，仅仅支持主键模型的表                                                                                   |
| save_mode_create_template   | string  | no   | 参见表下方的说明                     | 参见表下方的说明                                                                                                            |
| starrocks.config            | map     | no   | -                            | stream load `data_desc`参数                                                                                           |
| http_socket_timeout_ms      | int     | no   | 180000                       | http socket超时时间，默认为3分钟                                                                                              |
| schema_save_mode            | Enum    | no   | CREATE_SCHEMA_WHEN_NOT_EXIST | 在同步任务打开之前，针对目标端已存在的表结构选择不同的处理方法                                                                                     |
| data_save_mode              | Enum    | no   | APPEND_DATA                  | 在同步任务打开之前，针对目标端已存在的数据选择不同的处理方法                                                                                      |
| custom_sql                  | String  | no   | -                            | 当data_save_mode设置为CUSTOM_PROCESSING时，必须同时设置CUSTOM_SQL参数。CUSTOM_SQL的值为可执行的SQL语句，在同步任务开启前SQL将会被执行                     |

### save_mode_create_template

StarRocks数据接收器使用模板，在需求需要的时候也可以修改模板，并结合上游数据类型和结构生成表的创建语句来自动创建StarRocks表。当前仅在多表模式下有效。

默认模板如下：

```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table_name}` (
${rowtype_primary_key},
${rowtype_fields}
) ENGINE=OLAP
PRIMARY KEY (${rowtype_primary_key})
COMMENT '${comment}'
DISTRIBUTED BY HASH (${rowtype_primary_key})PROPERTIES (
"replication_num" = "1"
)
```

在模板中添加自定义字段，比如说加上`id`字段的修改模板如下：

```sql
CREATE TABLE IF NOT EXISTS `${database}`.`${table_name}`
(   
    id,
    ${rowtype_fields}
) ENGINE = OLAP 
    COMMENT '${comment}'
    DISTRIBUTED BY HASH (${rowtype_primary_key})
    PROPERTIES
(
    "replication_num" = "1"
);
```

StarRocks数据接收器根据上游数据自动获取相应的信息来填充模板，并且会移除`rowtype_fields`中的id字段信息。使用此方法可用来为自定义字段修改类型及相关属性。

可以使用的占位符有：

- database: 上游数据模式的库名称
- table_name: 上游数据模式的表名称
- rowtype_fields: 上游数据模式的所有字段信息，连接器会将字段信息自动映射到StarRocks对应的类型
- rowtype_primary_key: 上游数据模式的主键信息，结果可能是列表
- rowtype_unique_key: 上游数据模式的唯一键信息，结果可能是列表
- comment: 上游数据模式的注释信息

### table [string]

使用选项参数`database`和`table-name`自动生成SQL，并接收上游输入数据写入StarRocks中。

此选项与 `query` 是互斥的，具具有更高的优先级。

table选项参数可以填入一任意表名，这个名字最终会被用作目标表的表名，并且支持变量（`${table_name}`，`${schema_name}`）。
替换规则如下：`${schema_name}` 将替换传递给目标端的 SCHEMA 名称，`${table_name}` 将替换传递给目标端的表名。

例如：
1. test_${schema_name}_${table_name}_test
2. sink_sinktable
3. ss_${table_name}

### schema_save_mode[Enum]

在同步任务打开之前，针对目标端已存在的表结构选择不同的处理方法。可选值有：  
`RECREATE_SCHEMA` ：不存在的表会直接创建，已存在的表会删除并根据参数重新创建  
`CREATE_SCHEMA_WHEN_NOT_EXIST` ：忽略已存在的表，不存在的表会直接创建  
`ERROR_WHEN_SCHEMA_NOT_EXIST` ：当有不存在的表时会直接报错  
`IGNORE` ：忽略对表的处理

### data_save_mode[Enum]

在同步任务打开之前，针对目标端已存在的数据选择不同的处理方法。可选值有：
`DROP_DATA`： 保存数据库结构，但是会删除表中存量数据
`APPEND_DATA`：保存数据库结构和相关的表存量数据
`CUSTOM_PROCESSING`：自定义处理
`ERROR_WHEN_DATA_EXISTS`：当对应表存在数据时直接报错

### custom_sql[String]

当data_save_mode设置为CUSTOM_PROCESSING时，必须同时设置CUSTOM_SQL参数。CUSTOM_SQL的值为可执行的SQL语句，在同步任务开启前SQL将会被执行。

## 数据类型映射

| StarRocks数据类型 | SeaTunnel数据类型 |
|---------------|---------------|
| BOOLEAN       | BOOLEAN       |
| TINYINT       | TINYINT       |
| SMALLINT      | SMALLINT      |
| INT           | INT           |
| BIGINT        | BIGINT        |
| FLOAT         | FLOAT         |
| DOUBLE        | DOUBLE        |
| DECIMAL       | DECIMAL       |
| DATE          | STRING        |
| TIME          | STRING        |
| DATETIME      | STRING        |
| STRING        | STRING        |
| ARRAY         | STRING        |
| MAP           | STRING        |
| BYTES         | STRING        |

#### 支持导入的数据格式

StarRocks数据接收器支持的格式有CSV和JSON格式。

## 任务示例

### 简单示例

> 接下来给出一个示例，该示例包含多种数据类型的数据写入，且用户需要为目标端下游创建相应表

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
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    base-url = "jdbc:mysql://e2e_starRocksdb:9030/"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    batch_max_rows = 10
    starrocks.config = {
      format = "JSON"
      strip_outer_array = true
    }
  }
}
```

### 支持写入cdc变更事件（INSERT/UPDATE/DELETE）示例

```hocon
sink {
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    base-url = "jdbc:mysql://e2e_starRocksdb:9030/"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    ...
    
    // 支持upsert/delete事件的同步（需要将选项参数enable_upsert_delete设置为true），仅支持表引擎为主键模型
    enable_upsert_delete = true
  }
}
```

### JSON格式数据导入示例

```
sink {
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    base-url = "jdbc:mysql://e2e_starRocksdb:9030/"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    batch_max_rows = 10
    starrocks.config = {
      format = "JSON"
      strip_outer_array = true
    }
  }
}

```

### CSV格式数据导入示例

```
sink {
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    base-url = "jdbc:mysql://e2e_starRocksdb:9030/"
    username = root
    password = ""
    database = "test"
    table = "e2e_table_sink"
    batch_max_rows = 10
    starrocks.config = {
      format = "CSV"
      column_separator = "\\x01"
      row_delimiter = "\\x02"
    }
  }
}
```

### 使用save_mode的示例

```
sink {
  StarRocks {
    nodeUrls = ["e2e_starRocksdb:8030"]
    base-url = "jdbc:mysql://e2e_starRocksdb:9030/"
    username = root
    password = ""
    database = "test"
    table = "test_${schema_name}_${table_name}"
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
    data_save_mode="APPEND_DATA"
    batch_max_rows = 10
    starrocks.config = {
      format = "CSV"
      column_separator = "\\x01"
      row_delimiter = "\\x02"
    }
  }
}
```

## 变更日志

### 随后版本

- 增加StarRocks数据接收器
- [Improve] 将连接器自定义配置前缀的数据类型更改为Map [3719](https://github.com/apache/seatunnel/pull/3719)
- [Feature] 支持写入cdc变更事件(INSERT/UPDATE/DELETE) [3865](https://github.com/apache/seatunnel/pull/3865)

