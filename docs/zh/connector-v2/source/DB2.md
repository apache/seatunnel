# DB2

> JDBC DB2 Source连接器

## 支持引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 描述

通过JDBC读取外部数据源数据。

## 使用依赖关系

### 适用于 Spark/Flink 引擎

> 1. 您需要确保[jdbc驱动程序jar包](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc)已放置在目录`${SEATUNNEL_HOME}/plugins/`中。

### 适用于 SeaTunnel Zeta 引擎

> 1. 您需要确保[jdbc驱动程序jar包](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc)已放置在目录“${SEATUNNEL_HOME}/lib/”中。

## 关键特性

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [列映射](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)
- [x] [支持用户自定义拆分](../../concept/connector-v2-features.md)

> 支持查询SQL，可以实现映射效果。

## 支持的数据源信息

| 数据源 |                    支持版本                    |             驱动             |                Url                |                                 Maven                                 |
|------------|----------------------------------------------------------|--------------------------------|-----------------------------------|-----------------------------------------------------------------------|
| DB2        | 不同的依赖版本有不同的驱动程序类。| com.ibm.db2.jdbc.app.DB2Driver | jdbc:db2://127.0.0.1:50000/dbname | [下载](https://mvnrepository.com/artifact/com.ibm.db2.jcc/db2jcc) |

## 数据库相关性

> 请下载“Maven”对应的支持列表，并将其复制到“$SEATUNNEL_HOME/plugins/jdbc/lib/”工作目录<br/>
> 例如，DB2数据源：cp DB2-connector-java-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

|                                            DB2数据类型                                             | SeaTunnel 数据类型 |
|------------------------------------------------------------------------------------------------------|---------------------|---|
| BOOLEAN                                                                                              | BOOLEAN             |
| SMALLINT                                                                                             | SHORT               |
| INT<br/>INTEGER<br/>                                                                                 | INTEGER             |
| BIGINT                                                                                               | LONG                |
| DECIMAL<br/>DEC<br/>NUMERIC<br/>NUM                                                                  | DECIMAL(38,18)      |
| REAL                                                                                                 | FLOAT               |
| FLOAT<br/>DOUBLE<br/>DOUBLE PRECISION<br/>DECFLOAT                                                   | DOUBLE              |
| CHAR<br/>VARCHAR<br/>LONG VARCHAR<br/>CLOB<br/>GRAPHIC<br/>VARGRAPHIC<br/>LONG VARGRAPHIC<br/>DBCLOB | STRING              |
| BLOB                                                                                                 | BYTES               |
| DATE                                                                                                 | DATE                |
| TIME                                                                                                 | TIME                |
| TIMESTAMP                                                                                            | TIMESTAMP           |
| ROWID<br/>XML                                                                                        | Not supported yet   |

## 源选项

|             名称             |    类型    | 必需 |     默认值     |                                                                                                                            描述                                                                                                                            |
|------------------------------|------------|----------|-----------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                          | String     | 是      | -               | JDBC连接的URL。请参考案例：jdbc:db2://127.0.0.1:50000/dbname                                                                                                                                                                                |
| driver                       | String     | 是      | -               | 用于连接到远程数据源的jdbc类名，<br/>如果使用db2，则值为`com.ibm.db2.jdbc.app.DB2Driver`。                                                                                                                                 |
| user                         | String     | 否       | -               | 连接实例用户名                                                                                                                                                                                                                                     |
| password                     | String     | 否       | -               | 连接实例密码                                                                                                                                                                                                                                      |
| query                        | String     | 是      | -               | 查询语句                                                                                                                                                                                                                                                   |
| connection_check_timeout_sec | Int        | 否       | 30              | 等待用于验证连接的数据库操作完成的时间（秒）                                                                                                                                                               |
| partition_column             | String     | 否       | -               | 并行分区的列名，只支持数值类型，只支持数字类型主键，只能配置一列。                                                                                                                    |
| partition_lower_bound        | BigDecimal | 否       | -               | 扫描的partition_column最小值，如果未设置，SeaTunnel将查询数据库获取最小值。                                                                                                                                                                  |
| partition_upper_bound        | BigDecimal | 否       | -               | 扫描的partition_column最大值，如果没有设置，SeaTunnel将查询数据库获取最大值。                                                                                                                                                                  |
| partition_num                | Int        | 否      | job parallelism | 分区计数的数量，只支持正整数。默认值是作业并行性                                                                                                                                                                    |
| fetch_size                   | Int        | 否       | 0               | 对于返回大量对象的查询，您可以配置查询中使用的行提取大小，通过减少满足选择条件所需的数据库请求次数来提高性能。0表示使用jdbc默认值。 |
| properties                   | Map        | 否       | -               | 其他连接配置参数，当属性和URL具有相同的参数时，优先级由驱动程序的特定实现决定。例如，在MySQL中，属性优先于URL。                    |
| common-options               |            | 否       | -               | source插件常用参数，详见[Source common Options]（../source-common-options.md）                                                                                                                                                 |

### 小贴士

> 如果未设置partition_column，它将以单并发运行，如果设置了partition_column，它将根据任务的并发度并行执行。

## 任务示例

### 简单:

> 此示例以单并行方式在您的测试“database”中查询类型容器（type_bin）'table'的16条数据。并查询其所有字段。您还可以指定要查询哪些字段以将最终输出到控制台。

```
# 定义运行时环境
env {
  parallelism = 2
  job.mode = "BATCH"
}
source{
    Jdbc {
        url = "jdbc:db2://127.0.0.1:50000/dbname"
        driver = "com.ibm.db2.jdbc.app.DB2Driver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        query = "select * from table_xxx"
    }
}

transform {
    # 如果你想了解更多关于如何配置seatunnel的信息，并查看transform插件的完整列表,
    # 请前往 https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
    Console {}
}
```

### 并行度：

> 并行读取您的查询表，利用您配置的分片字段以及分片数据。若您希望读取整个表，您可以采取此操作。
```
source {
    Jdbc {
        url = "jdbc:db2://127.0.0.1:50000/dbname"
        driver = "com.ibm.db2.jdbc.app.DB2Driver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        # 根据需要定义查询逻辑
        query = "select * from type_bin"
        # 并行分片读取字段
        partition_column = "id"
        # 碎片数量
        partition_num = 10
    }
}
```

### 并行边界:

> 在查询的上下界范围内指定数据更为高效。根据您配置的上下边界读取数据源，效率更佳。

```
source {
    Jdbc {
        url = "jdbc:db2://127.0.0.1:50000/dbname"
        driver = "com.ibm.db2.jdbc.app.DB2Driver"
        connection_check_timeout_sec = 100
        user = "root"
        password = "123456"
        # 根据需求定义查询逻辑
        query = "select * from type_bin"
        partition_column = "id"
        # 读取起始边界
        partition_lower_bound = 1
        # 读取结束边界
        partition_upper_bound = 500
        partition_num = 10
    }
}
```

