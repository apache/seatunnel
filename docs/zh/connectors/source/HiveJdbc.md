import ChangeLog from '../changelog/connector-jdbc.md';

# HiveJdbc

> JDBC Hive 源连接器

## 支持Hive版本

- 确定支持3.1.3和3.1.2，其他版本需要测试。

## 超时参数支持

`socket_timeout_ms` 和 `connect_timeout_ms` 参数已在 **Hive 3.2.0+** 版本上测试验证。对于更早的版本(包括 3.1.x)，这些参数暂未验证。参数会被传递给 JDBC 驱动,但实际效果取决于使用的 Hive 版本。

## 支持这些引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 关键特性

- [x] [批](../../introduction/concepts/connector-v2-features.md)
- [ ] [流](../../introduction/concepts/connector-v2-features.md)
- [ ] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行性](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户自定义split](../../introduction/concepts/connector-v2-features.md)

> 支持查询SQL，可以实现投影效果。

## 描述

通过标准 JDBC 接口读取 Apache Hive 中的数据。连接器使用 HiveServer2 JDBC 驱动（`org.apache.hive.jdbc.HiveDriver`）将配置的 `query` 提交给 HiveServer2 执行并读取结果。相较于直接读取 HDFS 文件的 [Hive 源](Hive.md)，HiveJdbc 把所有 I/O 委托给 HiveServer2，更适合 SeaTunnel Worker 端无法直接访问 Metastore 或 HDFS 的场景。同时支持 Kerberos 认证。

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
| url                          | String     | 是  | -               | JDBC 连接的 URL。参考示例：`jdbc:hive2://localhost:10000/default`，指向 HiveServer2 端点。 |
| driver                       | String     | 是  | -               | 用于连接到远程数据源的 JDBC 类名。对于 Hive，值为 `org.apache.hive.jdbc.HiveDriver`。 |
| username                     | String     | 否  | -               | 连接实例用户名。 |
| password                     | String     | 否  | -               | 连接实例密码。 |
| query                        | String     | 是  | -               | 查询语句。HiveServer2 返回的结果集结构即为输出结构。 |
| connection_check_timeout_sec | Int        | 否  | 30              | 等待用于验证连接的数据库操作完成的时间（秒）。 |
| socket_timeout_ms            | Int        | 否  | 86400000        | 从服务器读取数据的 Socket 超时时间（毫秒）。设置为 `0` 表示无超时。已在 Hive 3.2.0+ 测试，更早版本暂未验证。 |
| connect_timeout_ms           | Int        | 否  | 86400000        | 建立到服务器的连接超时时间（毫秒）。设置为 `0` 表示无超时。已在 Hive 3.2.0+ 测试，更早版本暂未验证。 |
| partition_column             | String     | 否  | -               | 并行分区的列名，仅支持数值类型主键，且只能配置一列。 |
| partition_lower_bound        | BigDecimal | 否  | -               | 扫描的分区列最小值。如果未设置，SeaTunnel 将查询数据库获取最小值。 |
| partition_upper_bound        | BigDecimal | 否  | -               | 扫描的分区列最大值。如果未设置，SeaTunnel 将查询数据库获取最大值。 |
| partition_num                | Int        | 否  | job parallelism | 分区数量，仅支持正整数。默认值是作业并行数。 |
| fetch_size                   | Int        | 否  | 0               | 对于返回大量行的查询，可配置 JDBC 一次获取的行数，通过减少访问数据库的次数来提升性能。`0` 表示使用 JDBC 驱动默认。 |
| common-options               |            | 否  | -               | 源插件常用参数，请参考 [源通用选项](../common-options/source-common-options.md)。 |
| use_kerberos                 | Boolean    | 否  | false           | 是否启用 Kerberos 认证。 |
| kerberos_principal           | String     | 否  | -               | 当 `use_kerberos = true` 时，设置 Kerberos 主体，例如 `test_user@REALM`。 |
| kerberos_keytab_path         | String     | 否  | -               | 当 `use_kerberos = true` 时，设置 Kerberos keytab 文件路径，例如 `/home/test/test_user.keytab`。 |
| krb5_path                    | String     | 否  | /etc/krb5.conf  | 当 `use_kerberos = true` 时，设置 `krb5.conf` 路径，例如 `/seatunnel/krb5.conf`，或保留默认 `/etc/krb5.conf`。 |

### 提示

>如果未设置partition_column，它将以单并发运行，如果设置了partition_column，它将根据任务的并发性并行执行。当您的分片读取字段是bigint（及以上）等大数字类型并且数据分布不均匀时，建议将并行级别设置为1，以确保
数据倾斜问题已得到解决

## 任务示例

### 简单任务

> 此示例以单并行方式查询测试数据库中表 `type_bin` 的 16 条数据，并查询其所有字段。您也可以指定要查询的字段，最终输出到控制台。

```hocon
# 定义运行时环境
env {
  parallelism = 2
  job.mode = "BATCH"
}
source {
    Jdbc {
        url = "jdbc:hive2://localhost:10000/default"
        driver = "org.apache.hive.jdbc.HiveDriver"
        connection_check_timeout_sec = 100
        query = "select * from type_bin limit 16"
    }
}

transform {
    # If you would like to get more information about how to configure seatunnel and see full list of transform plugins,
    # please go to https://seatunnel.apache.org/docs/transforms/sql
}

sink {
    Console {}
}
```

### 并行任务

> 使用配置的分片字段并行读取查询表。如果需要读取整张表，可使用此模式。

```hocon
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

> 通过指定分区列的取值上下界可以更高效地读取数据。当取值集中时，建议显式指定范围。

```hocon
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

### 通过 Kerberos 读取

```hocon
source {
    Jdbc {
        url = "jdbc:hive2://hive-server:10000/default;principal=hive/_HOST@REALM"
        driver = "org.apache.hive.jdbc.HiveDriver"
        query = "select * from type_bin"
        use_kerberos = true
        kerberos_principal = "test_user@REALM"
        kerberos_keytab_path = "/home/test/test_user.keytab"
        krb5_path = "/etc/krb5.conf"
    }
}
```

## 修改日志

<ChangeLog />