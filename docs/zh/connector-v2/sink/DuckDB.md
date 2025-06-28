# DuckDB Sink

> DuckDB Sink 连接器

## 描述

DuckDB Sink 连接器以高性能和可靠性将数据写入 DuckDB 数据库。DuckDB 是一个专为分析型工作负载优化的进程内 SQL OLAP 数据库管理系统。此连接器支持基于文件和内存的 DuckDB 数据库，为分析管道、数据仓库和实时处理场景提供高效的数据接入。

**主要特性：**
- 高性能列式存储与出色的压缩性能
- 支持回滚的 ACID 兼容事务
- 通过模式推断自动创建表
- 为大型数据集优化的批量加载
- 支持复杂数据类型和嵌套结构

## 核心功能

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [更新插入](../../concept/connector-v2-features.md)
- [x] [变更数据捕获](../../concept/connector-v2-features.md)
- [x] [支持多表写入](../../concept/connector-v2-features.md)

## 数据类型映射

SeaTunnel 数据类型自动映射到 DuckDB 数据类型，映射关系如下：

| SeaTunnel 数据类型 | DuckDB 数据类型                | 说明                                         |
|----------------|--------------------------------|----------------------------------------------|
| BOOLEAN        | BOOLEAN                        |                                              |
| TINYINT        | TINYINT                        |                                              |
| SMALLINT       | SMALLINT                       |                                              |
| INT            | INTEGER                        |                                              |
| BIGINT         | BIGINT                         |                                              |
| FLOAT          | FLOAT                          |                                              |
| DOUBLE         | DOUBLE                         |                                              |
| DECIMAL(p,s)   | DECIMAL(p,s)                   | 保持精度和标度                               |
| STRING         | VARCHAR                        | 长度自动确定                                 |
| BYTES          | BLOB                           |                                              |
| DATE           | DATE                           |                                              |
| TIME           | TIME                           |                                              |
| TIMESTAMP      | TIMESTAMP                      |                                              |
| ARRAY          | T[]                            | 支持嵌套数组                                 |
| ROW            | STRUCT                         | 保持命名字段                                 |
| MAP<K,V>       | MAP(K,V)                       | 保持键值映射                                 |

## 配置选项

| 名称                         | 类型     | 必填 | 默认值  | 描述                                           |
|------------------------------|---------|------|--------|------------------------------------------------|
| url                          | String  | 是   | -      | JDBC 连接 URL                                  |
| driver                       | String  | 是   | -      | JDBC 驱动类名                                  |
| user                         | String  | 否   | -      | 数据库用户名                                    |
| password                     | String  | 否   | -      | 数据库密码                                      |
| database                     | String  | 否   | main   | 目标数据库名称                                  |
| table                        | String  | 是   | -      | 目标表名称                                      |
| schema                       | String  | 否   | main   | 目标模式名称                                    |
| connection_check_timeout_sec | Int     | 否   | 30     | 连接验证超时时间                                |
| batch_size                   | Int     | 否   | 1000   | 批量操作的批次大小                              |
| primary_keys                 | Array   | 否   | -      | 更新插入操作的主键列                            |
| max_retries                  | Int     | 否   | 3      | 最大重试次数                                    |
| retry_backoff_multiplier_ms  | Int     | 否   | 1000   | 重试退避乘数                                    |
| max_retry_backoff_ms         | Int     | 否   | 10000  | 最大重试退避时间                                |
| is_exactly_once              | Boolean | 否   | false  | 启用精确一次处理                                |
| generate_sink_sql            | Boolean | 否   | false  | 自动生成表模式                                  |
| xa_data_source_class_name    | String  | 否   | -      | 事务用 XA DataSource 类                        |
| max_commit_attempts          | Int     | 否   | 3      | 最大提交重试次数                                |
| transaction_timeout_sec      | Int     | 否   | 300    | 事务超时时间                                    |
| connection_pool_size         | Int     | 否   | 1      | 连接池最大连接数                                |
| enable_upsert                | Boolean | 否   | false  | 启用更新插入模式                                |
| save_mode                    | Enum    | 否   | append | 数据保存模式                                    |
| auto_create_table            | Boolean | 否   | false  | 表不存在时自动创建                              |
| schema_save_mode             | Enum    | 否   | CREATE_SCHEMA_WHEN_NOT_EXIST | 模式创建模式     |
| common-options               |         | 否   | -      | 通用 Sink 连接器选项                           |

### driver [String]

用于连接 DuckDB 的 JDBC 驱动类名。

- **必填**：是
- **值**：`org.duckdb.DuckDBDriver`
- **注意**：确保 DuckDB JDBC 驱动在类路径中可用

### user [String]

DuckDB 认证用户名。

- **必填**：否
- **默认值**：空（无需认证）
- **注意**：基于文件的 DuckDB 数据库通常不需要认证

### password [String]

DuckDB 认证密码。

- **必填**：否
- **默认值**：空
- **安全性**：建议使用环境变量或安全配置存储

### url [String]

DuckDB 数据库的 JDBC 连接 URL。

- **必填**：是
- **格式**：
  - 文件数据库：`jdbc:duckdb:/path/to/database.db`
  - 内存数据库：`jdbc:duckdb:`
  - 读写模式：`jdbc:duckdb:/path/to/database.db?access_mode=read_write`
- **性能参数**：
  - `threads=N`：设置工作线程数
  - `memory_limit=XGB`：设置内存限制
  - `max_memory=XGB`：设置最大内存使用量
- **示例**：
  - `jdbc:duckdb:/data/warehouse.db`
  - `jdbc:duckdb:/tmp/analytics.db?threads=4&memory_limit=2GB`

### database [String]

DuckDB 中的目标数据库名称。

- **必填**：否
- **默认值**：`main`
- **注意**：DuckDB 使用 'main' 作为默认数据库名称

### table [String]

数据写入的目标表名称。

- **必填**：是
- **格式**：简单表名或 schema.table
- **自动创建**：当 `generate_sink_sql` 为 true 时启用
- **示例**：`user_events` 或 `analytics.user_events`

### schema [String]

DuckDB 中的目标模式名称。

- **必填**：否
- **默认值**：`main`
- **注意**：当表指定时未包含模式前缀时使用

### connection_check_timeout_sec [Int]

数据库连接验证超时时间。

- **必填**：否
- **默认值**：30 秒
- **范围**：1-300 秒
- **性能**：较低值提供更快的故障检测

### batch_size [Int]

每批次插入操作的行数。

- **必填**：否
- **默认值**：1000
- **范围**：100-50000
- **性能**：
  - 较大批次提高吞吐量
  - 较小批次减少内存使用
  - 最佳范围：大多数情况下 1000-10000

### primary_keys [Array]

组成更新插入操作主键的列名。

- **必填**：否（更新插入模式时需要）
- **格式**：列名数组
- **用途**：启用 UPSERT（INSERT OR REPLACE）操作
- **示例**：`["id"]` 或 `["user_id", "event_date"]`

### max_retries [Int]

失败操作的最大重试次数。

- **必填**：否
- **默认值**：3
- **范围**：0-10
- **行为**：重试间使用指数退避

### retry_backoff_multiplier_ms [Int]

重试退避策略的基础延迟乘数。

- **必填**：否
- **默认值**：1000 毫秒
- **范围**：100-10000
- **计算**：延迟 = 乘数 × (2 ^ 尝试次数)

### max_retry_backoff_ms [Int]

重试尝试间的最大延迟。

- **必填**：否
- **默认值**：10000 毫秒
- **范围**：1000-300000
- **目的**：防止重试循环中的过度延迟

### is_exactly_once [Boolean]

使用 XA 事务启用精确一次处理语义。

- **必填**：否
- **默认值**：false
- **影响**：确保数据一致性但可能影响性能
- **注意**：需要 XA 兼容配置

### generate_sink_sql [Boolean]

基于源模式自动生成表创建 SQL。

- **必填**：否
- **默认值**：false
- **行为**：如果表不存在则创建
- **模式检测**：从源数据推断数据类型

### xa_data_source_class_name [String]

分布式事务的 XA DataSource 类名。

- **必填**：否（当 is_exactly_once=true 时需要）
- **值**：通常为 `org.duckdb.DuckDBDataSource`
- **用途**：启用两阶段提交协议

### max_commit_attempts [Int]

事务提交操作的最大尝试次数。

- **必填**：否
- **默认值**：3
- **范围**：1-10
- **用途**：提供临时提交失败的弹性

### transaction_timeout_sec [Int]

事务完成允许的最大时间。

- **必填**：否
- **默认值**：300 秒
- **范围**：10-3600 秒
- **影响**：防止长时间运行的事务锁

### connection_pool_size [Int]

连接池中的最大数据库连接数。

- **必填**：否
- **默认值**：1
- **范围**：1-100
- **注意**：DuckDB 支持有限的并发连接
- **最佳实践**：对文件数据库保持较低值（1-5）

### enable_upsert [Boolean]

启用更新插入（INSERT OR REPLACE）操作。

- **必填**：否
- **默认值**：false
- **依赖**：需要配置 `primary_keys`
- **性能**：对大型数据集可能比 INSERT 慢

### save_mode [Enum]

目标表的数据保存策略。

- **必填**：否
- **默认值**：`append`
- **值**：
  - `append`：插入新数据（默认）
  - `overwrite`：插入前截断表
  - `error_if_exists`：表已存在时失败
  - `ignore_if_exists`：表已存在时跳过

### auto_create_table [Boolean]

目标表不存在时自动创建。

- **必填**：否
- **默认值**：false
- **行为**：启用时基于源模式创建表
- **依赖**：与 `schema_save_mode` 配置协同工作
- **注意**：对于 DuckDB 嵌入式数据库场景至关重要

### schema_save_mode [Enum]

处理表模式创建和管理的策略。

- **必填**：否
- **默认值**：`CREATE_SCHEMA_WHEN_NOT_EXIST`
- **值**：
  - `CREATE_SCHEMA_WHEN_NOT_EXIST`：表不存在时创建（推荐）
  - `RECREATE_SCHEMA`：删除并重新创建表
  - `ERROR_WHEN_SCHEMA_NOT_EXIST`：表不存在时失败
  - `IGNORE`：跳过模式操作
- **最佳实践**：与 `auto_create_table = true` 配合使用以实现无缝操作

### common options

Sink 插件通用参数，详情请参考 [Sink Common Options](common-options.md)。

## 配置示例

### 基础配置

```hocon
sink {
  DuckDB {
    url = "jdbc:duckdb:/tmp/analytics.db"
    driver = "org.duckdb.DuckDBDriver"
    table = "user_events"
    generate_sink_sql = true
    auto_create_table = true
    schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
  }
}
```

### 高性能批量加载

```hocon
sink {
  DuckDB {
    url = "jdbc:duckdb:/data/warehouse.db?threads=8&memory_limit=4GB"
    driver = "org.duckdb.DuckDBDriver"
    table = "large_dataset"
    batch_size = 10000
    connection_pool_size = 2
    save_mode = "overwrite"
  }
}
```

### 更新插入配置

```hocon
sink {
  DuckDB {
    url = "jdbc:duckdb:/data/customer_data.db"
    driver = "org.duckdb.DuckDBDriver"
    table = "customers"
    primary_keys = ["customer_id"]
    enable_upsert = true
    batch_size = 5000
    generate_sink_sql = true
  }
}
```

### 精确一次处理

```hocon
sink {
  DuckDB {
    url = "jdbc:duckdb:/data/financial.db"
    driver = "org.duckdb.DuckDBDriver"
    table = "transactions"
    is_exactly_once = true
    xa_data_source_class_name = "org.duckdb.DuckDBDataSource"
    transaction_timeout_sec = 600
    max_commit_attempts = 5
  }
}
```

### 多模式配置

```hocon
sink {
  DuckDB {
    url = "jdbc:duckdb:/data/enterprise.db"
    driver = "org.duckdb.DuckDBDriver"
    database = "main"
    schema = "analytics"
    table = "user_behavior"
    generate_sink_sql = true
    save_mode = "append"
  }
}
```

### 内存高速处理

```hocon
sink {
  DuckDB {
    url = "jdbc:duckdb:"
    driver = "org.duckdb.DuckDBDriver"
    table = "temp_results"
    batch_size = 20000
    generate_sink_sql = true
    save_mode = "overwrite"
  }
}
```

## 事务管理

### ACID 合规性

DuckDB 提供完整的 ACID 合规性，具有以下保证：

- **原子性**：事务中的所有操作要么全部成功要么全部失败
- **一致性**：事务后数据库保持有效状态
- **隔离性**：并发事务不会相互干扰
- **持久性**：已提交的事务在系统故障后仍然持续存在

### 事务模式

1. **自动提交模式**（默认）：
   - 每个批次自动提交
   - 批量操作性能最快
   - 回滚能力有限

2. **精确一次模式**：
   - 使用 XA 事务保证
   - 启用跨系统一致性
   - 性能略有降低

3. **批量事务模式**：
   - 在事务中分组多个批次
   - 平衡性能和一致性
   - 可配置事务边界

## 性能调优

### 内存优化
- 在连接 URL 中设置适当的 `memory_limit`
- 根据可用内存和行大小使用 `batch_size`
- 在大数据加载期间监控内存使用

### 批量加载优化
- 使用更大的 `batch_size` 获得更好的吞吐量（5000-20000）
- 在批量加载期间禁用不必要的索引
- 考虑使用 `save_mode=overwrite` 进行全表刷新

### 连接管理
- 对文件数据库保持较低的 `connection_pool_size`
- 为大型操作使用适当的 `transaction_timeout_sec`
- 监控连接池指标并相应调整

### 查询优化
- 利用 DuckDB 的列式存储优势
- 使用适当的数据类型最小化存储
- 考虑时间序列数据的分区策略

## 安全考虑

### 文件系统安全
- 确保数据库文件具有适当的文件权限
- 使用安全的文件路径，避免全局可读位置
- 实施定期备份和恢复程序
- 考虑敏感数据的静态加密

### 访问控制
- 实施应用级访问控制
- 对数据库操作使用最小权限原则
- 审计写入操作和数据修改
- 验证输入数据以防止注入攻击

### 事务安全
- 对关键财务数据使用精确一次模式
- 实施适当的错误处理和回滚程序
- 监控异常事务模式
- 确保高容量操作期间的备份一致性

## 故障排除

### 常见问题

**表创建失败：**
```
错误：表已存在
解决方案：根据情况设置 save_mode = "append" 或 "overwrite"
```

**内存问题：**
```
错误：批量插入期间内存不足
解决方案：减少 batch_size 或在 URL 中增加 memory_limit
```

**事务超时：**
```
错误：事务超时已超过
解决方案：增加 transaction_timeout_sec 或优化数据处理
```

**连接池耗尽：**
```
错误：无法从池中获取连接
解决方案：增加 connection_pool_size 或减少并发操作
```

**数据类型不匹配：**
```
错误：无法转换数据类型
解决方案：验证数据类型映射或启用 generate_sink_sql
```

**表不存在：**
```
错误：表 'table_name' 不存在！
解决方案：启用 auto_create_table = true 和 schema_save_mode = "CREATE_SCHEMA_WHEN_NOT_EXIST"
```

**自动建表失败：**
```
错误：自动建表失败
解决方案：确保配置了 database 参数（对于 DuckDB 可以为空）
```

### 调试技巧

1. **启用详细日志记录：**
   ```hocon
   env {
     job.mode = "BATCH"
     "seatunnel.logs.level" = "DEBUG"
   }
   ```

2. **测试表创建：**
   ```sql
   -- 验证表结构
   DESCRIBE your_table;
   SELECT COUNT(*) FROM your_table;
   ```

3. **监控性能：**
   ```sql
   -- 检查数据库统计信息
   SELECT * FROM duckdb_tables();
   SELECT * FROM duckdb_columns();
   ```

4. **验证事务：**
   ```sql
   -- 检查活动事务
   SELECT * FROM duckdb_transactions();
   ```

## 最佳实践

1. **模式设计**：使用适当的数据类型和主键
2. **批次大小调整**：根据数据特征优化 batch_size
3. **错误处理**：实施健壮的重试和回滚策略
4. **监控**：跟踪性能指标和错误率
5. **测试**：在负载下验证数据完整性和性能
6. **备份**：维护生产数据的定期备份计划
7. **资源管理**：监控内存和磁盘使用
8. **安全性**：实施适当的访问控制和加密

## 限制

- **并发写入者**：文件数据库的并发写入访问有限
- **流处理**：不支持原生流处理（仅批处理）
- **分布式事务**：分布式事务支持有限
- **模式演化**：动态模式更改可能需要重新创建表
- **大对象**：非常大的 BLOB 对象可能影响性能

## 高级功能

### 复杂数据类型
```hocon
# 嵌套结构示例
sink {
  DuckDB {
    url = "jdbc:duckdb:/data/complex.db"
    driver = "org.duckdb.DuckDBDriver"
    table = "nested_data"
    generate_sink_sql = true
    # 自动支持 ARRAY、STRUCT、MAP 类型
  }
}
```

### 分区加载
```sql
-- DuckDB 支持分区表
CREATE TABLE events_partitioned (
  id BIGINT,
  event_date DATE,
  data VARCHAR
) PARTITION BY (event_date);
```

### 压缩优化
```hocon
# 启用压缩以提高存储效率
sink {
  DuckDB {
    url = "jdbc:duckdb:/data/compressed.db?enable_http_metadata_cache=true"
    driver = "org.duckdb.DuckDBDriver"
    table = "compressed_data"
    # DuckDB 自动应用压缩
  }
}
```

## 更新日志

### 2.3.0-beta (2023-03-15)
- DuckDB Sink 连接器首次发布
- 支持基础数据类型映射
- 基于文件的数据库连接

### 2.3.1+ (2023-04+)
- 持续的稳定性改进和错误修复
- 增强错误处理
- 性能优化

*注意：详细发布说明请参考 [SeaTunnel 官方发布页面](https://github.com/apache/seatunnel/releases)* 