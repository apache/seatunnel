import ChangeLog from '../changelog/connector-clickhouse.md';

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
- [x] [并行度](../../concept/connector-v2-features.md)
- [x] [支持用户自定义拆分](../../concept/connector-v2-features.md)

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
| partition_column      | String  | 否       |                        | 并行读取数据表时的分片字段，目前支持数字、日期、时间和字符串类型，如果不填写，则数据读取默认只有1个分片，即不进行并行读取，此时跟并行读取的相关配置都不会生效 |
| partition_num | Integer | 否 | 10 | 并行读取数据表时的分片数量 |
| partition_lower_bound | String | 否 |  | 并行读取进行分片时的下限值，根据分片字段填入对应数据类型，分片算法会以此作为分片的下限范围，如果partition_upper_bound没有填写，则不生效 |
| partition_upper_bound | String | 否 |  | 并行读取进行分片时的上限值，根据分片字段填入对应数据类型，分片算法会以此作为分片的上限范围，如果partition_lower_bound没有填写，则不生效 |
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

> 小提示
>
> 1.[SeaTunnel 部署文档](../../start-v2/locally/deployment.md).

## 关键特性解析

### 并行读取

#### 分片算法

并行读取分片切分策略，根据分区字段的类型不同，主要分为两大类： 

**1.数字类**

数字类又包含纯数字类和日期类：

（1）纯数字类

基于下限和上限，计算出分区大小，并根据分区数进行切分（最后一个分区可能会小于分区大小）。

（2）时间类

时间类在主要是包括Date和DateTime两大类，但不管哪一类，都会先转换为其数值大小，然后切分算法跟纯数字类一样，切分出分区之后，如果字段是Date类型，则会使用ClickHouse的`toDate()`函数将分区数值进行转换，而如果是DateTime类型，则会使用ClickHouse的`toDateTime64()`函数将分区数值进行转换

> 无论是纯数值类型还是时间类型，如果未指定下限或上限，将会请求数据库获取最大值和最小值。

**2.字符串类**

对于字符串，指定上下限无效，切分算法会对分区字段根据分区数进行取模，以切分数据。



在根据以上分片算法对数据进行分片之后，相应分片会平均发送给对应并行度的Reader，进而实现并行读取ClickHouse数据表，从而极大提高ClickHouse数据读取效率。

#### 配置案例

**1.纯数字类**

```
source {
  Clickhouse {
    host = "clickhouse:8123"
    database = "default"
    sql = "select * from parallel_source_table"
    username = "default"
    password = ""
    plugin_output = "parallel_source_table"
    partition_column = "id"
    partition_num = 3
    # partition_lower_bound = 1
    # partition_upper_bound = 10
  }
}
```

**2.时间类**

Date类：

```
source {
  Clickhouse {
    host = "clickhouse:8123"
    database = "default"
    sql = "select * from parallel_source_table"
    username = "default"
    password = ""
    plugin_output = "parallel_source_table"
    partition_column = "enrollment_date"
    partition_num = 3
    # partition_lower_bound = "2024-05-20"
    # partition_upper_bound = "2024-06-20"
  }
}
```

DateTime类：

```
source {
  Clickhouse {
    host = "clickhouse:8123"
    database = "default"
    sql = "select * from parallel_source_table"
    username = "default"
    password = ""
    plugin_output = "parallel_source_table"
    partition_column = "date"
    partition_num = 3
    # partition_lower_bound = "2024-05-20 08:30:00"
    # partition_upper_bound = "2024-06-19 13:30:00"
  }
}
```

**3.字符串类**

```
source {
  Clickhouse {
    host = "clickhouse:8123"
    database = "default"
    sql = "select * from parallel_source_table"
    username = "default"
    password = ""
    plugin_output = "parallel_source_table"
    partition_column = "email"
    partition_num = 3
  }
}
```

## 变更日志

<ChangeLog />