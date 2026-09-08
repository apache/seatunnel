# 模式演进
模式演进是指数据表的Schema可以改变，数据同步任务可以自动适应新的表结构的变化而无需其他操作。

## 已支持的引擎

- Zeta

## 已支持的模式变更事件类型

- `ADD COLUMN`
- `DROP COLUMN`
- `RENAME COLUMN`
- `MODIFY COLUMN`

## 已支持的连接器

### 源
[Mysql-CDC](../../connectors/source/MySQL-CDC.md)
[Oracle-CDC](../../connectors/source/Oracle-CDC.md)

### 目标
[Jdbc-Mysql](../../connectors/sink/Jdbc.md)
[Jdbc-Oracle](../../connectors/sink/Jdbc.md)
[Jdbc-Postgres](../../connectors/sink/Jdbc.md)
[Jdbc-Dameng](../../connectors/sink/Jdbc.md)
[Jdbc-SqlServer](../../connectors/sink/Jdbc.md)
[StarRocks](../../connectors/sink/StarRocks.md)
[Doris](../../connectors/sink/Doris.md)
[Paimon](../../connectors/sink/Paimon.md#模式演变)
[Elasticsearch](../../connectors/sink/Elasticsearch.md#模式演变)
[Redis](../../connectors/sink/Redis.md#模式演变)

注意: 
* 目前模式演进不支持transform。不同类型数据库(Oracle-CDC -> Jdbc-Mysql)的模式演进目前不支持ddl中列的默认值。

* 当你使用Oracle-CDC时，你不能使用用户名`SYS`或`SYSTEM`来修改表结构，否则ddl事件将被过滤，这可能导致模式演进不起作用；
另外，如果你的表名以`ORA_TEMP_`开头，也会有相同的问题。

* 早期版本的`达梦`数据库不支持将`Varchar`类型字段更改为`Text`类型字段。

## 启用Schema evolution功能
在CDC源连接器中模式演进默认是关闭的。你需要在CDC连接器中配置`schema-changes.enabled = true`来启用它。

## 多库多表路由

只要每张上游表都能稳定映射到一个明确的物理下游表，模式演进就可以和多库多表任务一起工作。SeaTunnel 会在连接器启动前完成 Sink 占位符替换，因此你可以结合 [Sink 参数占位符](./sink-options-placeholders.md) 中的 `${database_name}`、`${schema_name}`、`${table_name}` 做路由。

推荐做法：

- 如果希望不同上游库的表彼此隔离，请把它们路由到不同的物理下游表。
- 如果需要并行写入，可继续开启 `multi_table_sink_replica`；模式变更会按最终渲染出的物理下游表维度协调执行。
- 如果你有意把多张上游表写入同一张物理下游表，请自行保证这些表的 schema 兼容，并确保主键不会冲突。

### 示例：不同源库中的同名表 -> 不同下游库中的同名表

```hocon
source {
  MySQL-CDC {
    database-names = ["shop_a", "shop_b"]
    table-names = ["shop_a.products", "shop_b.products"]
    url = "jdbc:mysql://mysql-host:3306"
    schema-changes.enabled = true
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://mysql-host:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"
    generate_sink_sql = true
    database = "${database_name}_sink"
    table = "${table_name}"
    primary_keys = ["id"]
    multi_table_sink_replica = 2
  }
}
```

在这个例子里，`shop_a.products` 会写入 `shop_a_sink.products`，`shop_b.products` 会写入 `shop_b_sink.products`。

如果两张源表之后都执行了 `ALTER TABLE products ADD COLUMN add_column1 VARCHAR(64), ADD COLUMN add_column2 INT` 这类 DDL，SeaTunnel 会分别把 schema 变更应用到 `shop_a_sink.products` 和 `shop_b_sink.products`，并继续保证每张下游表只接收自己所属源库的数据。

### 示例：写入同一个下游库，但拆成不同下游表

```hocon
sink {
  jdbc {
    url = "jdbc:mysql://mysql-host:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"
    generate_sink_sql = true
    database = "ods"
    table = "${database_name}_${table_name}"
    primary_keys = ["id"]
  }
}
```

在这个例子里，`shop_a.products` 会写入 `ods.shop_a_products`，`shop_b.products` 会写入 `ods.shop_b_products`。

### 示例：用通配符捕获多库多表

```hocon
source {
  MySQL-CDC {
    table-pattern = "sales_.*\\..*"
    url = "jdbc:mysql://mysql-host:3306"
    schema-changes.enabled = true
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://mysql-host:3306"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "root"
    password = "123456"
    generate_sink_sql = true
    database = "ods"
    table = "${database_name}_${table_name}"
    primary_keys = ["${primary_key}"]
  }
}
```

## 示例

### Mysql-CDC -> Jdbc-Mysql
```
env {
  # You can set engine configuration here
  parallelism = 5
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  read_limit.bytes_per_second=7000000
  read_limit.rows_per_second=400
}

source {
  MySQL-CDC {
    server-id = 5652-5657
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    
    schema-changes.enabled = true
  }
}

sink {
  jdbc {
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "st_user_sink"
    password = "mysqlpw"
    generate_sink_sql = true
    database = shop
    table = mysql_cdc_e2e_sink_table_with_schema_change_exactly_once
    primary_keys = ["id"]
    is_exactly_once = true
    xa_data_source_class_name = "com.mysql.cj.jdbc.MysqlXADataSource"
  }
}
```

### Oracle-cdc -> Jdbc-Oracle
```
env {
  # You can set engine configuration here
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  Oracle-CDC {
    plugin_output = "customers"
    username = "dbzuser"
    password = "dbz"
    database-names = ["ORCLCDB"]
    schema-names = ["DEBEZIUM"]
    table-names = ["ORCLCDB.DEBEZIUM.FULL_TYPES"]
    url = "jdbc:oracle:thin:@oracle-host:1521/ORCLCDB"
    source.reader.close.timeout = 120000
    connection.pool.size = 1
    
    schema-changes.enabled = true
  }
}

sink {
    Jdbc {
      plugin_input = "customers"
      driver = "oracle.jdbc.driver.OracleDriver"
      url = "jdbc:oracle:thin:@oracle-host:1521/ORCLCDB"
      user = "dbzuser"
      password = "dbz"
      generate_sink_sql = true
      database = "ORCLCDB"
      table = "DEBEZIUM.FULL_TYPES_SINK"
      batch_size = 1
      primary_keys = ["ID"]
      connection.pool.size = 1
    }
}
```

### Oracle-cdc -> Jdbc-Mysql
```
env {
  # You can set engine configuration here
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  # This is a example source plugin **only for test and demonstrate the feature source plugin**
  Oracle-CDC {
    plugin_output = "customers"
    username = "dbzuser"
    password = "dbz"
    database-names = ["ORCLCDB"]
    schema-names = ["DEBEZIUM"]
    table-names = ["ORCLCDB.DEBEZIUM.FULL_TYPES"]
    url = "jdbc:oracle:thin:@oracle-host:1521/ORCLCDB"
    source.reader.close.timeout = 120000
    connection.pool.size = 1
    
    schema-changes.enabled = true
  }
}

sink {
  jdbc {
    plugin_input = "customers"
    url = "jdbc:mysql://oracle-host:3306/oracle_sink"
    driver = "com.mysql.cj.jdbc.Driver"
    user = "st_user_sink"
    password = "mysqlpw"
    generate_sink_sql = true
    # You need to configure both database and table
    database = oracle_sink
    table = oracle_cdc_2_mysql_sink_table
    primary_keys = ["ID"]
  }
}
```

### Mysql-cdc -> StarRocks
```
env {
  # You can set engine configuration here
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    
    schema-changes.enabled = true
  }
}

sink {
  StarRocks {
    nodeUrls = ["starrocks_cdc_e2e:8030"]
    username = "root"
    password = ""
    database = "shop"
    table = "${table_name}"
    url = "jdbc:mysql://starrocks_cdc_e2e:9030/shop"
    max_retries = 3
    enable_upsert_delete = true
    schema_save_mode="RECREATE_SCHEMA"
    data_save_mode="DROP_DATA"
    save_mode_create_template = """
    CREATE TABLE IF NOT EXISTS shop.`${table_name}` (
        ${rowtype_primary_key},
        ${rowtype_fields}
        ) ENGINE=OLAP
        PRIMARY KEY (${rowtype_primary_key})
        DISTRIBUTED BY HASH (${rowtype_primary_key})
        PROPERTIES (
                "replication_num" = "1",
                "in_memory" = "false",
                "enable_persistent_index" = "true",
                "replicated_storage" = "true",
                "compression" = "LZ4"
          )
    """
  }
}
```

### Mysql-CDC -> Doris
```
env {
  # You can set engine configuration here
  parallelism = 1
  job.mode = "STREAMING"
  checkpoint.interval = 5000
}

source {
  MySQL-CDC {
    server-id = 5652-5657
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    schema-changes.enabled = true
  }
}

sink {
  Doris {
    fenodes = "doris_e2e:8030"
    username = "root"
    password = ""
    database = "shop"
    table = "products"
    sink.label-prefix = "test-cdc"
    sink.enable-2pc = "true"
    sink.enable-delete = "true"
    doris.config {
      format = "json"
      read_json_by_line = "true"
    }
  }
}
```

> **注意（schema 演进 + 2PC）：** 当 `sink.enable-2pc = "true"` 时，Doris schema 演进仅支持 `format = "json"`，因为 JSON load 会按列名匹配。CSV 等位置敏感格式在启用 2PC 的 schema 演进场景下会被运行时拒绝。请使用 `format = "json"`，或设置 `sink.enable-2pc = "false"`，让 sink 可以在应用 DDL 前先 flush 已缓冲的数据。

### Mysql-CDC -> Jdbc-Postgres
```hocon
env {
  # You can set engine configuration here
  parallelism = 5
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  read_limit.bytes_per_second=7000000
  read_limit.rows_per_second=400
}

source {
  MySQL-CDC {
    server-id = 5652-5657
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"

    schema-changes.enabled = true
  }
}

sink {
  jdbc {
    url = "jdbc:postgresql://postgresql:5432/shop"
    driver = "org.postgresql.Driver"
    user = "postgres"
    password = "postgres"
    generate_sink_sql = true
    database = shop
    table = "public.sink_table_with_schema_change"
    primary_keys = ["id"]

    # Validate ddl update for sink writer multi replica
    multi_table_sink_replica = 2
  }
}
```

### Mysql-CDC -> Jdbc-Dameng
```hocon
env {
  # You can set engine configuration here
  parallelism = 5
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  read_limit.bytes_per_second=7000000
  read_limit.rows_per_second=400
}

source {
  MySQL-CDC {
    server-id = 5652-5657
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"

    schema-changes.enabled = true
  }
}

sink {
  jdbc {
    url = "jdbc:dm://e2e_dmdb:5236"
    driver = "dm.jdbc.driver.DmDriver"
    connection_check_timeout_sec = 1000
    user = "SYSDBA"
    password = "SYSDBA"
    generate_sink_sql = true
    database = "DAMENG"
    table = "SYSDBA.sink_table_with_schema_change"
    primary_keys = ["id"]

    # Validate ddl update for sink writer multi replica
    multi_table_sink_replica = 2
  }
}
```

### Mysql-CDC -> Jdbc-SqlServer
```hocon
env {
  # You can set engine configuration here
  parallelism = 5
  job.mode = "STREAMING"
  checkpoint.interval = 5000
  read_limit.bytes_per_second=7000000
  read_limit.rows_per_second=400
}

source {
  MySQL-CDC {
    server-id = 5652-5657
    username = "st_user_source"
    password = "mysqlpw"
    table-names = ["shop.products"]
    url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"

    schema-changes.enabled = true
  }
}

sink {
  jdbc {
    url = "jdbc:sqlserver://e2e_sqlserver:1433"
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    user = "sa"
    password = "paanssy1234$"
    generate_sink_sql = true
    database = master
    table = "dbo.sink_table_with_schema_change"
    primary_keys = ["id"]

    # Validate ddl update for sink writer multi replica
    multi_table_sink_replica = 2
  }
}
```
