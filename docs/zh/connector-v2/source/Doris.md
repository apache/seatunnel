# Doris

> Doris 源连接器

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要功能

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [x] [支持用户自定义分片](../../concept/connector-v2-features.md)
- [x] [支持多表读](../../concept/connector-v2-features.md)

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
| doris.request.read.timeout.ms    | int    | no       | 30000      |                                                                                                     |
| doris.request.connect.timeout.ms | int    | no       | 30000      |                                                                                                     |
| query-port                       | string | no       | 9030       | Doris查询端口                                                                                         |
| doris.request.query.timeout.s    | int    | no       | 3600       | Doris扫描数据的超时时间，单位秒                                                                          |
| table_list                       | string | 否       | -           | 表清单                                                                                               |

表清单配置:

|               名称                |  类型   | 是否必须  |  默认值     |                                             描述                                                     |
|----------------------------------|--------|----------|------------|-----------------------------------------------------------------------------------------------------|
| database                         | string | yes      | -          | 数据库                                                                                               |
| table                            | string | yes      | -          | 表名                                                                                                |
| doris.read.field                 | string | no       | -          | 选择要读取的Doris表字段                                                                                |
| doris.filter.query               | string | no       | -          | 数据过滤. 格式："字段 = 值", 例如：doris.filter.query = "F_ID > 2"                                       |
| doris.batch.size                 | int    | no       | 1024       | 每次能够从BE中读取到的最大行数                                                                           |
| doris.exec.mem.limit             | long   | no       | 2147483648 | 单个be扫描请求可以使用的最大内存。默认内存为2G（2147483648）                                                |
 
注意: 当此配置对应于单个表时，您可以将table_list中的配置项展平到外层。

### 提示

> 不建议随意修改高级参数

## 例子

### 单表
> 这是一个从doris读取数据后，输出到控制台的例子：

```
env {
  parallelism = 2
  job.mode = "BATCH"
}
source{
  Doris {
      fenodes = "doris_e2e:8030"
      username = root
      password = ""
      database = "e2e_source"
      table = "doris_e2e_table"
  }
}

transform {
    # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transform/sql
}

sink {
    Console {}
}
```

使用`doris.read.field`参数来选择需要读取的Doris表字段：

```
env {
  parallelism = 2
  job.mode = "BATCH"
}
source{
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
    # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transform/sql
}

sink {
    Console {}
}
```

使用`doris.filter.query`来过滤数据，参数值将作为过滤条件直接传递到doris：

```
env {
  parallelism = 2
  job.mode = "BATCH"
}
source{
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
    # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transform/sql
}

sink {
    Console {}
}
```
### 多表
```
env{
  parallelism = 1
  job.mode = "BATCH"
}

source{
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
          },
          {
            database = "st_source_1"
            table = "doris_table_1"
          }
      ]
  }
}

transform {}

sink{
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
          format="json"
          read_json_by_line="true"
      }
  }
}
```
