import ChangeLog from '../changelog/connector-jdbc.md';

# MariaDB

> JDBC MariaDB 输出连接器

## 描述

通过 JDBC 将数据写入 MariaDB。

## 支持 MariaDB 版本

- 10.2 / 10.3 / 10.4 / 10.5 / 10.6 / 10.7 / 10.8 / 10.9 / 10.10 / 10.11 / 11.0 / 11.1 / 11.2 / 11.3 / 11.4

## 支持的引擎

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## 主要功能

- [x] [精确一次](../../introduction/concepts/connector-v2-features.md)
- [x] [cdc](../../introduction/concepts/connector-v2-features.md)
- [x] [主键](../../introduction/concepts/connector-v2-features.md)

## 示例

```hocon
sink {
  Jdbc {
    url = "jdbc:mariadb://localhost:3306/test"
    driver = "org.mariadb.jdbc.Driver"
    user = "root"
    password = "123"
    query = "INSERT INTO test_table (id, name) VALUES (?, ?)"
  }
}
```

<ChangeLog />
