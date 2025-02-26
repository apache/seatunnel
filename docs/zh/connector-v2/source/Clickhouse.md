# Clickhouse

> Clickhouse source 连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 核心特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [列映射](../../concept/connector-v2-features.md)
- [ ] [并行度](../../concept/connector-v2-features.md)
- [ ] [支持用户自定义拆分](../../concept/connector-v2-features.md)

> 支持查询SQL，可以实现投影效果。

## 描述

用于从Clickhouse读取数据。

## 支持的数据源信息

为了使用 Clickhouse 连接器，需要以下依赖项。它们可以通过 install-plugin.sh 或从 Maven 中央存储库下载。

| 数据源        | 支持的版本     | 依赖                                                                               |
|------------|--------------------|------------------------------------------------------------------------------------------|
| Clickhouse | universal          | [Download](https://mvnrepository.com/artifact/org.apache.seatunnel/connector-clickhouse) |

## 数据类型映射

| Clickhouse 数据类型                                                              | SeaTunnel 数据类型 |
|-----------------------------------------------------------------------------------------------------------------------------------------------|---------------------|
| String / Int128 / UInt128 / Int256 / UInt256 / Point / Ring / Polygon MultiPolygon                                                            | STRING              |
| Int8 / UInt8 / Int16 / UInt16 / Int32                                                                                                         | INT                 |
| UInt64 / Int64 / IntervalYear / IntervalQuarter / IntervalMonth / IntervalWeek / IntervalDay / IntervalHour / IntervalMinute / IntervalSecond | BIGINT              |
| Float64                                                                                                                                       | DOUBLE              |
| Decimal                                                                                                                                       | DECIMAL             |
| Float32                                                                                                                                       | FLOAT               |
| Date                                                                                                                                          | DATE                |
| DateTime                                                                                                                                      | TIME                |
| Array                                                                                                                                         | ARRAY               |
| Map                                                                                                                                           | MAP                 |

## Source 选项

|       名称                   |   类型    | 是否必须 |  默认值         |                                                                                                                                                 描述                                                                                                                                                 |
|-------------------|--------|----------|------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host              | String | 是      | -                      | `ClickHouse` 集群地址, 格式是`host:port` , 允许多个`hosts`配置. 例如 `"host1:8123,host2:8123"` .                                                                                                                                                                    |
| database          | String | 是      | -                      | The `ClickHouse` 数据库名称.                                                                                                                                                                                                                                                                                  |
| sql               | String | 是      | -                      | 用于通过Clickhouse服务搜索数据的查询sql.                                                                                                                                                                                                                                                 |
| username          | String | 是      | -                      | `ClickHouse` user 用户账号.                                                                                                                                                                                                                                                                                 |
| password          | String | 是      | -                      | `ClickHouse` user 用户密码.                                                                                                                                                                                                                                                                                 |
| clickhouse.config | Map    | 否       | -                      | 除了上述必须由 `clickhouse-jdbc` 指定的必填参数外，用户还可以指定多个可选参数，这些参数涵盖了 `clickhouse-jdbc` 提供的所有[参数](https://github.com/ClickHouse/clickhouse-jdbc/tree/master/clickhouse-client#configuration). |
| server_time_zone  | String | 否       | ZoneId.systemDefault() | 数据库服务中的会话时区。如果未设置，则使用ZoneId.systemDefault（）设置服务时区.                                                                                                                                                                                |
| common-options    |        | 否       | -                      | 源插件常用参数，详见 [源通用选项](../source-common-options.md).                                                                                                                                                                                          |

## 如何创建Clickhouse数据同步作业

以下示例演示了如何创建数据同步作业，该做作业从Clickhouse读取数据并在本地客户端上打印:

```bash
# 设置要执行的任务的基本配置
env {
  parallelism = 10
  job.mode = "BATCH"
}

# 创建连接到Clickhouse的源
source {
  Clickhouse {
    host = "localhost:8123"
    database = "default"
    sql = "select * from test where age = 20 limit 100"
    username = "xxxxx"
    password = "xxxxx"
    server_time_zone = "UTC"
    plugin_output = "test"
    clickhouse.config = {
      "socket_timeout": "300000"
    }
  }
}

# 控制台打印读取的Clickhouse数据
sink {
  Console {
    parallelism = 1
  }
}
```

### 小提示

> 1.[SeaTunnel 部署文档](../../start-v2/locally/deployment.md).

