import ChangeLog from '../changelog/connector-jdbc.md';

# MariaDB

> JDBC MariaDB 源连接器

## 描述

通过 JDBC 从 MariaDB 读取外部数据源数据。

## 支持 MariaDB 版本

- 10.2 / 10.3 / 10.4 / 10.5 / 10.6 / 10.7 / 10.8 / 10.9 / 10.10 / 10.11 / 11.0 / 11.1 / 11.2 / 11.3 / 11.4

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 需要的依赖项

### 对于 Spark/Flink 引擎

> 1. 您需要确保 [mariadb jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/org.mariadb.jdbc/mariadb-java-client) 已放置在目录 `${SEATUNNEL_HOME}/plugins/` 中。

### 对于 SeaTunnel Zeta 引擎

> 1. 您需要确保 [mariadb jdbc 驱动程序 jar 包](https://mvnrepository.com/artifact/org.mariadb.jdbc/mariadb-java-client) 已放置在目录 `${SEATUNNEL_HOME}/lib/` 中。

## 主要功能

- [x] [批处理](../../introduction/concepts/connector-v2-features.md)
- [ ] [流处理](../../introduction/concepts/connector-v2-features.md)
- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [列投影](../../introduction/concepts/connector-v2-features.md)
- [x] [并行度](../../introduction/concepts/connector-v2-features.md)
- [x] [支持用户定义的拆分](../../introduction/concepts/connector-v2-features.md)
- [x] [支持多表读取](../../introduction/concepts/connector-v2-features.md)

## 支持的数据源信息

| 数据源 |                    支持的版本                   |          驱动器          |                  网址                  | Maven下载链接                                                           |
|-----|---------------------------------------------------------|--------------------------|---------------------------------------|---------------------------------------------------------------------|
| MariaDB | MariaDB 10.2+ | org.mariadb.jdbc.Driver | jdbc:mariadb://localhost:3306/test | [下载](https://mvnrepository.com/artifact/org.mariadb.jdbc/mariadb-java-client) |

## 数据类型映射

| MariaDB 数据类型                                                                              | SeaTunnel 数据类型                                                                                              |
|---------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------|
| BIT(1)<br/>BOOLEAN<br/>BOOL<br/>TINYINT(1) (类型收窄)                                        | BOOLEAN                                                                                                         |
| TINYINT                                                                                     | BYTE                                                                                                            |
| TINYINT UNSIGNED<br/>SMALLINT                                                               | SMALLINT                                                                                                        |
| SMALLINT UNSIGNED<br/>MEDIUMINT<br/>MEDIUMINT UNSIGNED<br/>INT<br/>INTEGER<br/>YEAR         | INT                                                                                                             |
| INT UNSIGNED<br/>INTEGER UNSIGNED<br/>BIGINT                                                | BIGINT                                                                                                          |
| BIGINT UNSIGNED                                                                             | DECIMAL(20,0)                                                                                                   |
| DECIMAL(x,y)(精度 <= 38)                                                                    | DECIMAL(x,y)                                                                                                    |
| DECIMAL(x,y)(精度 > 38)                                                                     | DECIMAL(38,18)                                                                                                  |
| DECIMAL UNSIGNED                                                                            | DECIMAL(精度+1, 标度)                                                                                            |
| FLOAT<br/>FLOAT UNSIGNED                                                                    | FLOAT                                                                                                           |
| DOUBLE<br/>DOUBLE UNSIGNED                                                                  | DOUBLE                                                                                                          |
| CHAR<br/>VARCHAR<br/>TINYTEXT<br/>MEDIUMTEXT<br/>TEXT<br/>LONGTEXT<br/>JSON<br/>ENUM<br/>SET<br/>UUID<br/>INET4<br/>INET6 | STRING                                                                            |
| DATE                                                                                        | DATE                                                                                                            |
| TIME(s)                                                                                     | TIME(s)                                                                                                         |
| DATETIME                                                                                    | TIMESTAMP (无时区)                                                                                               |
| TIMESTAMP(s)                                                                                | TIMESTAMP_TZ                                                                                                    |
| TINYBLOB<br/>MEDIUMBLOB<br/>BLOB<br/>LONGBLOB<br/>BINARY<br/>VARBINARY<br/>BIT(n)<br/>GEOMETRY | BYTES                                                                                                           |

## 源选项

| 名称                                       | 类型       | 是否必须 | 默认值          | 描述                                                                                                                                                            |
|--------------------------------------------|------------|----------|-----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| url                                        | String     | 是       | -               | JDBC 连接的 URL，例如：`jdbc:mariadb://localhost:3306/test`                                                                                                    |
| driver                                     | String     | 是       | -               | JDBC 驱动类名：`org.mariadb.jdbc.Driver`                                                                                                                       |
| user / username                            | String     | 否       | -               | 数据库连接用户名                                                                                                                                                |
| password                                   | String     | 否       | -               | 数据库连接密码                                                                                                                                                  |
| query                                      | String     | 否       | -               | 查询语句。未配置 `table_path` 或 `table_list` 时必须配置此参数。                                                                                                |
| table_path                                 | String     | 否       | -               | 完整表路径，例如 `testdb.table1`                                                                                                                                |
| table_list                                 | Array      | 否       | -               | 要读取的表列表。                                                                                                                                                |
| fetch_size                                 | Int        | 否       | 0               | 查询数据行获取大小。                                                                                                                                            |
| int_type_narrowing                         | Boolean    | 否       | true            | 是否将 `tinyint(1)` 收窄为 BOOLEAN 类型。                                                                                                                       |
| common-options                             |            | 否       | -               | 源插件通用参数。                                                                                                                                                |

## 示例

```hocon
source {
  Jdbc {
    url = "jdbc:mariadb://localhost:3306/test"
    driver = "org.mariadb.jdbc.Driver"
    user = "root"
    password = "123"
    query = "select * from test_table"
  }
}
```

<ChangeLog/>
