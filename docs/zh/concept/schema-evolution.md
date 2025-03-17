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
[Mysql-CDC](https://github.com/apache/seatunnel/blob/dev/docs/en/connector-v2/source/MySQL-CDC.md)
[Oracle-CDC](https://github.com/apache/seatunnel/blob/dev/docs/en/connector-v2/source/Oracle-CDC.md)

### 目标
[Jdbc-Mysql](https://github.com/apache/seatunnel/blob/dev/docs/zh/connector-v2/sink/Jdbc.md)
[Jdbc-Oracle](https://github.com/apache/seatunnel/blob/dev/docs/zh/connector-v2/sink/Jdbc.md)
[Jdbc-Postgres](https://github.com/apache/seatunnel/blob/dev/docs/zh/connector-v2/sink/Jdbc.md)
[Jdbc-Dameng](https://github.com/apache/seatunnel/blob/dev/docs/zh/connector-v2/sink/Jdbc.md)
[StarRocks](https://github.com/apache/seatunnel/blob/dev/docs/zh/connector-v2/sink/StarRocks.md)
[Doris](https://github.com/apache/seatunnel/blob/dev/docs/zh/connector-v2/sink/Doris.md)
[Paimon](https://github.com/apache/seatunnel/blob/dev/docs/zh/connector-v2/sink/Paimon.md#模式演变)
[Elasticsearch](https://github.com/apache/seatunnel/blob/dev/docs/zh/connector-v2/sink/Elasticsearch.md#模式演变)

注意: 
* 目前模式演进不支持transform。不同类型数据库(Oracle-CDC -> Jdbc-Mysql)的模式演进目前不支持ddl中列的默认值。

* 当你使用Oracle-CDC时，你不能使用用户名`SYS`或`SYSTEM`来修改表结构，否则ddl事件将被过滤，这可能导致模式演进不起作用；
另外，如果你的表名以`ORA_TEMP_`开头，也会有相同的问题。

* 早期版本的`达梦`数据库不支持将`Varchar`类型字段更改为`Text`类型字段。

## 启用Schema evolution功能
在CDC源连接器中模式演进默认是关闭的。你需要在CDC连接器中配置`schema-changes.enabled = true`来启用它。

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
    base-url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    
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
    base-url = "jdbc:oracle:thin:@oracle-host:1521/ORCLCDB"
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
    base-url = "jdbc:oracle:thin:@oracle-host:1521/ORCLCDB"
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
    base-url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
    
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
    base-url = "jdbc:mysql://starrocks_cdc_e2e:9030/shop"
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
    base-url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"
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
    base-url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"

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
    base-url = "jdbc:mysql://mysql_cdc_e2e:3306/shop"

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