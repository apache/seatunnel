# Sink 参数占位符

## 介绍

SeaTunnel 提供了 Sink 参数占位符自动替换功能，可让您通过占位符获取上游表元数据。

当您需要动态获取上游表元数据（例如多表写入）时，此功能至关重要。

本文档将指导您如何使用这些占位符以及如何有效地利用它们。

## 支持的引擎

> SeaTunnel Zeta<br/>
> Flink<br/>
> Spark<br/>

## 占位符变量

占位符主要通过以下表达式实现:

- `${database_name}`
  - 用于获取上游表中的数据库名称
  - 也可以通过表达式指定默认值：`${database_name:default_my_db}`
- `${schema_name}`
  - 用于获取上游表中的 schema 名称
  - 也可以通过表达式指定默认值：`${schema_name:default_my_schema}`
- `${table_name}`
  - 用于获取上游表中的 table 名称
  - 也可以通过表达式指定默认值：`${table_name:default_my_table}`
- `${schema_full_name}`
  - 用于获取上游表中的 schema 全路径名称，包含 database/schema 名称
- `${table_full_name}`
  - 用于获取上游表中的 table 全路径名称，包含 database/schema/table 名称
- `${primary_key}`
  - 用于获取上游表中的主键字段名称列表
- `${unique_key}`
  - 用于获取上游表中的唯一键字段名称列表
- `${field_names}`
  - 用于获取上游表中的所有字段名称列表
- `${comment}`
  - 用于获取上游表中的表注释

## 配置

*先决条件*:
- 确认 Sink 连接器已经支持了 `TableSinkFactory` API

### 配置示例 1

```hocon
env {
  // ignore...
}
source {
  MySQL-CDC {
    // ignore...
  }
}

transform {
  // ignore...
}

sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"

    database = "${database_name}_test"
    table = "${table_name}_test"
    primary_keys = ["${primary_key}"]
  }
}
```

### 配置示例 2

```hocon
env {
  // ignore...
}
source {
  Oracle-CDC {
    // ignore...
  }
}

transform {
  // ignore...
}

sink {
  jdbc {
    url = "jdbc:mysql://localhost:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"

    database = "${schema_name}_test"
    table = "${table_name}_test"
    primary_keys = ["${primary_key}"]
  }
}
```

占位符的替换将在连接器启动之前完成，确保 Sink 参数在使用前已准备就绪。
若该占位符变量没有被替换，则可能是上游表元数据缺少该选项，例如：
- `mysql` source 连接器不包含 `${schema_name}` 元数据
- `oracle` source 连接器不包含 `${databse_name}` 元数据
- ...
