# 模式演进
模式演进是指数据表的Schema可以改变，数据同步任务可以自动适应新的表结构的变化而无需其他操作。

## 已支持的引擎

- Zeta
- Flink

Spark 不支持 schema evolution，也不会执行 `schema-changes.behavior` 策略。Spark 作业请勿开启
`schema-changes.enabled`。

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

## Schema 变更行为策略

CDC Source 在配置 `schema-changes.enabled = true` 时，可以继续配置 `schema-changes.behavior`。
默认值是 `evolve`，因此已有作业只配置 `schema-changes.enabled = true` 时，会保持最接近现有语义的行为。
当 `schema-changes.enabled = false` 时，schema change event 不会发送到下游，该选项不会改变当前行为。
配置值不区分大小写；下面使用的小写形式是配置示例中的规范写法。

CDC 反序列化器会先应用 `schema-changes.include` 和 `schema-changes.exclude`，再执行 behavior
策略。完全被过滤的事件既不会更新 Source 输出行结构，也不会进入 behavior 策略。因此，例如
`strict` 不会因为一个已排除的事件而失败。

| 值 | 运行时契约 |
| --- | --- |
| `strict` | 一旦观察到 schema change event，立即让作业失败，并且不会尝试下游 schema 协调或 Sink 侧 schema 变更。 |
| `evolve` | 将受支持的 schema change event 转发到正常的 schema 协调路径。不支持的行结构变更和 Sink 侧 apply 失败都会让作业失败；不受支持的纯注释事件会在各 Sink 路径记录日志并丢弃，因为它们不影响行编码。 |
| `ignore` | 只在下游 schema 协调和 Sink 侧 schema evolution 之前丢弃 `ALTER_TABLE_COMMENT` 和 `ALTER_COLUMN_COMMENT`。ADD、DROP、RENAME、MODIFY COLUMN 会改变运行时行结构，因此会失败而不是被忽略。 |

行为矩阵：

| 场景 | `strict` | `evolve` | `ignore` |
| --- | --- | --- | --- |
| Source 发出受支持的 schema change 类型 | 在下游传播前失败 | 通过 Sink 协调并应用 | 仅在可以安全忽略时，在下游传播前丢弃 |
| Source 发出不支持的 schema change 类型 | 在下游传播前失败 | 每个 Sink 路径独立处理：Flink 在协调前的策略门禁记录并丢弃纯注释事件，Zeta 在协调后的 Sink 生命周期记录并丢弃；其他事件在 Sink 侧 apply 前失败 | 在协调前丢弃 `ALTER_TABLE_COMMENT` 和 `ALTER_COLUMN_COMMENT`；行结构变更失败 |
| Sink 支持 schema evolution | 不会到达 Sink | 通过 `SupportSchemaEvolutionSinkWriter` 应用 | 不会到达 Sink |
| Sink 不支持 schema evolution | 不会到达 Sink | 纯注释事件记录日志后丢弃；在一个版本的兼容窗口内，调用显式覆写的 deprecated 方法，继承默认 no-op 时记录告警并丢弃 | 不会到达 Sink |
| Sink apply 在运行时抛出异常 | 不会到达 Sink | 使用 Sink apply 错误让作业失败 | 不会到达 Sink |

升级说明：在 `evolve` 模式下，Sink writer 应实现 `SupportSchemaEvolutionSinkWriter` 来接收并应用
schema change event。在 deprecated 兼容窗口内，Zeta 单表、Zeta 多表以及两个 Flink sink 路径仍会调用 Sink writer 显式覆写的
`SinkWriter.applySchemaChange`，并记录迁移告警。为避免升级后直接破坏已有作业，在一个版本的兼容窗口内，
继承的默认 no-op 也会记录告警并丢弃事件；该兜底将在下一个版本移除。请将 Sink writer 迁移到
`SupportSchemaEvolutionSinkWriter`、关闭 `schema-changes.enabled`，或排除 Sink 不应接收的事件类型。
`schema-changes.behavior = ignore` 只适用于纯注释变更。

策略拒绝是确定性失败。Zeta 会将其标记为不可重试，Flink 会用 `SuppressRestartsException` 包装，
因此从同一 checkpoint 恢复时不会反复重放同一条被拒绝的 DDL。请修改为 `evolve` 并使用兼容的
Sink、关闭 `schema-changes.enabled`，或调整过滤规则后重新提交作业。

在 `evolve` 模式下，如果外部 DDL 已成功但随后的 checkpoint 未完成，恢复后 schema change event
可能再次投递。因此 `SupportSchemaEvolutionSinkWriter` 实现必须幂等应用事件。JDBC 实现会在重放
ADD、DROP、RENAME COLUMN 前检查当前 Sink schema；其他 Sink 必须为其声明支持的事件类型提供等价保证。

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
    schema-changes.behavior = evolve
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
