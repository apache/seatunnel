import ChangeLog from '../changelog/connector-doris.md';

# Doris

> Doris 源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要功能

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义分片](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表读](../../introduction/concepts/connector-v2-features.md)

## 描述

用于 Apache Doris 的源连接器。

## 依赖

### 对于 Spark/Flink

> 1. 你需要下载 [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 并添加到目录 `${SEATUNNEL_HOME}/plugins/`.

### 对于 SeaTunnel Zeta

> 1. 你需要下载 [jdbc driver jar package](https://mvnrepository.com/artifact/mysql/mysql-connector-java) 并添加到目录 `${SEATUNNEL_HOME}/lib/`.

## 支持的数据源信息

| 数据源      |          支持版本                      | 驱动   | Url | Maven |
|------------|--------------------------------------|--------|-----|-------|
| Doris      | 仅支持Doris2.0及以上版本.               | -      | -   | -     |

## 数据类型映射

|           Doris 数据类型               |                                                                 SeaTunnel 数据类型                                                                   |
|--------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| INT                                  | INT                                                                                                                                                 |
| TINYINT                              | TINYINT                                                                                                                                             |
| SMALLINT                             | SMALLINT                                                                                                                                            |
| BIGINT                               | BIGINT                                                                                                                                              |
| LARGEINT                             | STRING                                                                                                                                              |
| BOOLEAN                              | BOOLEAN                                                                                                                                             |
| DECIMAL                              | DECIMAL((Get the designated column's specified column size)+1,<br/>(Gets the designated column's number of digits to right of the decimal point.))) |
| FLOAT                                | FLOAT                                                                                                                                               |
| DOUBLE                               | DOUBLE                                                                                                                                              |
| CHAR<br/>VARCHAR<br/>STRING<br/>TEXT | STRING                                                                                                                                              |
| JSON                                 | STRING                                                                                                                                              |
| VARIANT                              | STRING                                                                                                                                              |
| DATE                                 | DATE                                                                                                                                                |
| DATETIME<br/>DATETIME(p)             | TIMESTAMP                                                                                                                                           |
| ARRAY                                | ARRAY                                                                                                                                               |

## 源选项

基础配置:

|               名称                |  类型   | 是否必须  |  默认值     |                                             描述                                                     |
|----------------------------------|--------|----------|------------|-----------------------------------------------------------------------------------------------------|
| fenodes                          | string | yes      | -          | FE 地址, 格式：`"fe_host:fe_http_port"`                                                               |
| username                         | string | yes      | -          | 用户名                                                                                               |
| password                         | string | yes      | -          | 密码                                                                                                 |
| doris.request.retries            | int    | no       | 3          | 请求Doris FE的重试次数                                                                                 |
| doris.request.read.timeout.ms    | int    | no       | 30000      | 请求 Doris BE 的 socket 读取超时时间。                                                                 |
| doris.request.connect.timeout.ms | int    | no       | 30000      | 请求 Doris FE 或 BE 的连接超时时间。                                                                    |
| query-port                       | int    | no       | 9030       | Doris 查询端口。                                                                                       |
| doris.request.query.timeout.s    | int    | no       | 3600       | Doris扫描数据的超时时间，单位秒                                                                          |
| doris.request.tablet.size        | int    | no       | Integer.MAX_VALUE | 每个 SeaTunnel split 包含的 Doris tablet 数量，最小值为 `1`。                                  |
| doris.deserialize.arrow.async    | boolean | no      | false      | 是否异步反序列化 Arrow 数据。                                                                           |
| doris.request.retriesdoris.deserialize.queue.size | int | no | 64 | 异步反序列化 Arrow 数据时使用的队列大小。该键名是连接器源码中实际的运行时选项 key（源代码存在已知拼写问题）；调优队列大小时请使用此精确 key。 |
| table_list                       | Array  | no       | -           | 要读取的 Doris 表清单。                                                                                |

表清单配置:

|               名称                |  类型   | 是否必须  |  默认值     |                                             描述                                                     |
|----------------------------------|--------|----------|------------|-----------------------------------------------------------------------------------------------------|
| database                         | string | yes      | -          | 数据库                                                                                               |
| table                            | string | yes      | -          | 表名                                                                                                |
| doris.read.field                 | string | no       | -          | 选择要读取的Doris表字段                                                                                |
| doris.filter.query               | string | no       | -          | 数据过滤. 格式："字段 = 值", 例如：doris.filter.query = "F_ID > 2"                                       |
| doris.request.tablet.size        | int    | no       | Integer.MAX_VALUE | 当前表每个 SeaTunnel split 包含的 Doris tablet 数量，最小值为 `1`。                              |
| doris.batch.size                 | int    | no       | 1024       | 每次能够从BE中读取到的最大行数                                                                           |
| doris.exec.mem.limit             | long   | no       | 2147483648 | 单个be扫描请求可以使用的最大内存。默认内存为2G（2147483648）                                                |
 
注意: 当此配置对应于单个表时，您可以将table_list中的配置项展平到外层。如果不配置 `table_list`，必须在 source 外层配置 `database` 和 `table`。

### 提示

> 不建议随意修改高级参数，除非你清楚其底层行为。上表中的默认值已经针对常见负载做了调优。

## 例子

### 单表

> 该示例从 Doris 单表读取数据，并写入 Console。

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Doris {
    fenodes = "doris_e2e:8030"
    username = root
    password = ""
    database = "e2e_source"
    table = "doris_e2e_table"
  }
}

transform {
  # 如果想了解如何配置 SeaTunnel 以及完整 transform 插件列表，请参考
  # https://seatunnel.apache.org/docs/transforms/sql
}

sink {
  Console {}
}
```

使用 `doris.read.field` 参数选择 Doris 表中需要读取的列：

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Doris {
    fenodes = "doris_e2e:8030"
    username = root
    password = ""
    database = "e2e_source"
    table = "doris_e2e_table"
    doris.read.field = "F_ID,F_INT,F_BIGINT,F_TINYINT,F_SMALLINT"
  }
}

transform {
  # 如果想了解如何配置 SeaTunnel 以及完整 transform 插件列表，请参考
  # https://seatunnel.apache.org/docs/transforms/sql
}

sink {
  Console {}
}
```

使用 `doris.filter.query` 过滤数据，该参数会作为谓词直接下推到 Doris：

```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  Doris {
    fenodes = "doris_e2e:8030"
    username = root
    password = ""
    database = "e2e_source"
    table = "doris_e2e_table"
    doris.filter.query = "F_ID > 2"
  }
}

transform {
  # 如果想了解如何配置 SeaTunnel 以及完整 transform 插件列表，请参考
  # https://seatunnel.apache.org/docs/transforms/sql
}

sink {
  Console {}
}
```

### 多表

> 该示例同时读取多张 Doris 表并写入 Doris Sink，`${table_name}` 占位符会按 `table_list` 中每条记录进行展开。

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Doris {
    fenodes = "xxxx:8030"
    username = root
    password = ""
    table_list = [
      {
        database = "st_source_0"
        table = "doris_table_0"
        doris.read.field = "F_ID,F_INT,F_BIGINT,F_TINYINT"
        doris.filter.query = "F_ID >= 50"
        doris.request.tablet.size = 1
        doris.exec.mem.limit = 2147483648
      },
      {
        database = "st_source_1"
        table = "doris_table_1"
      }
    ]
  }
}

transform {}

sink {
  Doris {
    fenodes = "xxxx:8030"
    schema_save_mode = "RECREATE_SCHEMA"
    username = root
    password = ""
    database = "st_sink"
    table = "${table_name}"
    sink.enable-2pc = "true"
    sink.label-prefix = "test_json"
    doris.config = {
      format = "json"
      read_json_by_line = "true"
    }
  }
}
```

## 变更日志

<ChangeLog />
