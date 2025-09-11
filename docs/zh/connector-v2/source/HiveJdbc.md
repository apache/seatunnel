import ChangeLog from '../changelog/connector-jdbc.md';

# HiveJdbc

> JDBC Hive 源连接器

## 支持Hive版本

- 确定支持3.1.3和3.1.2，其他版本需要测试。

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批](../../concept/connector-v2-features.md)
- [ ] [流](../../concept/connector-v2-features.md)
- [ ] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)
- [x] [并行性](../../concept/connector-v2-features.md)
- [x] [支持用户自定义split](../../concept/connector-v2-features.md)

> 支持查询SQL，可以实现投影效果。

## 描述

通过JDBC读取外部数据源数据。

## 支持的数据源信息

| 数据源  | 支持的版本                                                    | 驱动                              | 连接串                                  |                                  Maven                                   |
|------|----------------------------------------------------------|---------------------------------|--------------------------------------|--------------------------------------------------------------------------|
| Hive | 不同的依赖版本有不同的驱动程序类。 | org.apache.hive.jdbc.HiveDriver | jdbc:hive2://localhost:10000/default | [Download](https://mvnrepository.com/artifact/org.apache.hive/hive-jdbc) |

## 数据库相关性

> 请下载“Maven”对应的支持列表，并将其复制到"$SEATUNNEL_HOME/plugins/jdbc/lib/"
> 工作目录<br/>
> 例如，Hive数据源：cp Hive-jdbc-xxx.jar $SEATUNNEL_HOME/plugins/jdbc/lib/

## 数据类型映射

| Hive 数据类型                                                                                 | SeaTunnel 数据类型    |
|-------------------------------------------------------------------------------------------|-------------------|
| BOOLEAN                                                                                   | BOOLEAN           |
| TINYINT<br/> SMALLINT                                                                     | SHORT             |
| INT<br/>INTEGER                                                                           | INT               |
| BIGINT                                                                                    | LONG              |
| FLOAT                                                                                     | FLOAT             |
| DOUBLE<br/>DOUBLE PRECISION                                                               | DOUBLE            |
| DECIMAL(x,y)<br/>NUMERIC(x,y)<br/>(Get the designated column's specified column size.<38) | DECIMAL(x,y)      |
| DECIMAL(x,y)<br/>NUMERIC(x,y)<br/>(Get the designated column's specified column size.>38) | DECIMAL(38,18)    |
| CHAR<br/>VARCHAR<br/>STRING                                                               | STRING            |
| DATE                                                                                      | DATE              |
| DATETIME<br/>TIMESTAMP                                                                    | TIMESTAMP         |
| BINARY<br/>  ARRAY <br/>INTERVAL <br/>MAP   <br/>STRUCT<br/>UNIONTYPE                     | Not supported yet |

## 源配置项

| 参数名                          | 类型         | 必须 | 默认值             | 描述                                                                                                                          |
|------------------------------|------------|----|-----------------|-----------------------------------------------------------------------------------------------------------------------------|
| url                          | String     | 是  | -               | JDBC连接的URL。参考示例: jdbc:hive2://localhost:10000/default                                                                       |
| driver                       | String     | 是  | -               | 用于连接到远程数据源的jdbc类名，<br/> 如果使用Hive，则值为 `org.apache.hive.jdbc.HiveDriver`.                                                     |
| username                     | String     | 否  | -               | 连接实例用户名                                                                                                                     |
| password                     | String     | 否  | -               | 连接实例密码                                                                                                                      |
| query                        | String     | 是  | -               | 查询sql                                                                                                                       |
| connection_check_timeout_sec | Int        | 否  | 30              | 等待用于验证连接的数据库操作完成的时间（秒）                                                                                                      |
| partition_column             | String     | 否  | -               | 并行分区的列名，只支持数值类型，只支持数字类型主键，只能配置一列。                                                                                           |
| partition_lower_bound        | BigDecimal | 否  | -               | 扫描的分区列最小值，如果未设置，SeaTunnel将查询数据库获取最小值。                                                                                       |
| partition_upper_bound        | BigDecimal | 否  | -               | 扫描的分区列最大值，如果没有设置，SeaTunnel将查询数据库获取最大值。                                                                                      |
| partition_num                | Int        | 否  | job parallelism | 分区数量，仅支持正整数。 默认值是作业并行数                                                                                                      |
| fetch_size                   | Int        | 否 | 0               | 对于返回大量对象的查询，您可以配置查询中使用的行提取大小，通过减少满足选择条件所需的数据库查询次数来提高性能。0表示使用jdbc默认值。                                                        |
| common-options               |            | 否 | -               | 源插件常用参数，请参考 [源通用选项](../source-common-options.md) 详见                                                         |
| use_kerberos                 | Boolean    | 否 | no              | 是否启用Kerberos，默认值为false                                                                                |
| kerberos_principal           | String     | 否 | -               | 使用kerberos时，我们应该设置kerberos主体，例如"test_user@xxx".                                                   |
| kerberos_keytab_path         | String     | 否 | -               | 使用kerberos时，我们应该设置kerberos主体文件路径，如“/home/test/test_user.keytab”。                         |
| krb5_path                    | String     | 否 | /etc/krb5.conf  | 使用kerberos时，我们应该设置krb5路径文件路径，如“/seatunnel/krb5.conf”，或使用默认路径“/etc/krb5.conf”。 |

### 提示

>如果未设置partition_column，它将以单并发运行，如果设置了partition_column，它将根据任务的并发性并行执行。当您的分片读取字段是bigint（及以上）等大数字类型并且数据分布不均匀时，建议将并行级别设置为1，以确保
数据倾斜问题已得到解决

## 任务示例

### 简单任务

>此示例以单并行方式查询测试数据库中表type_bin的16条数据，并查询其所有字段。您还可以指定要查询哪些字段以将最终输出到控制台。

```
# 定义运行时环境
env {
  parallelism = 2
  job.mode = "BATCH"
}
source{
    Jdbc {
        url = "jdbc:hive2://localhost:10000/default"
        driver = "org.apache.hive.jdbc.HiveDriver"
        connection_check_timeout_sec = 100
        query = "select * from type_bin limit 16"
    }
}

transform {
    # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transform-v2/sql
}

sink {
    Console {}
}
```

### 并行任务

> 与您配置的分片字段和分片数据并行读取查询表如果您想读取整个表，可以这样做

```
source {
    Jdbc {
        url = "jdbc:hive2://localhost:10000/default"
        driver = "org.apache.hive.jdbc.HiveDriver"
        connection_check_timeout_sec = 100
        # Define query logic as required
        query = "select * from type_bin"
        # Parallel sharding reads fields
        partition_column = "id"
        # Number of fragments
        partition_num = 10
    }
}
```

### 并行度临界值

> 指定并行度的值在分区字段的值上下界之间，这样可以更高效的读取数据

```
source {
    Jdbc {
        url = "jdbc:hive2://localhost:10000/default"
        driver = "org.apache.hive.jdbc.HiveDriver"
        connection_check_timeout_sec = 100
        # Define query logic as required
        query = "select * from type_bin"
        partition_column = "id"
        # Read start boundary
        partition_lower_bound = 1
        # Read end boundary
        partition_upper_bound = 500
        partition_num = 10
    }
}
```

## 修改日志

<ChangeLog />