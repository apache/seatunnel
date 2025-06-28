# DuckDB Source

> DuckDB Source 连接器

## 描述

DuckDB Source 连接器用于从 DuckDB 数据库中读取数据。DuckDB 是一个进程内 SQL OLAP 数据库管理系统，特别适合分析型工作负载。此连接器支持基于文件和内存的 DuckDB 数据库，非常适合数据分析管道、数据迁移和实时数据处理场景。

**主要特性：**
- 高性能列式存储引擎
- 支持 ACID 合规性和事务处理
- 基于文件的数据库零配置设置
- 分析查询性能卓越
- 支持复杂 SQL 操作，包括窗口函数、CTE 和聚合

## 核心功能

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md) 
- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [列投影](../../concept/connector-v2-features.md)
- [x] [并行处理](../../concept/connector-v2-features.md)
- [x] [支持用户定义分片](../../concept/connector-v2-features.md)

## 数据类型映射

DuckDB 数据类型自动映射到 SeaTunnel 数据类型，映射关系如下：

| DuckDB 数据类型                | SeaTunnel 数据类型             |
|--------------------------------|--------------------------------|
| BOOLEAN                        | BOOLEAN                        |
| TINYINT                        | TINYINT                        |
| SMALLINT                       | SMALLINT                       |
| INTEGER                        | INT                            |
| BIGINT                         | BIGINT                         |
| FLOAT                          | FLOAT                          |
| DOUBLE                         | DOUBLE                         |
| DECIMAL(p,s)                   | DECIMAL(p,s)                   |
| VARCHAR(n)                     | STRING                         |
| CHAR(n)                        | STRING                         |
| TEXT                           | STRING                         |
| DATE                           | DATE                           |
| TIME                           | TIME                           |
| TIMESTAMP                      | TIMESTAMP                      |
| TIMESTAMP WITH TIME ZONE       | TIMESTAMP                      |
| BLOB                           | BYTES                          |
| ARRAY                          | ARRAY                          |
| STRUCT                         | ROW                            |
| MAP                            | MAP                            |

## 配置选项

| 名称                         | 类型     | 必填 | 默认值 | 描述                                           |
|------------------------------|---------|------|-------|------------------------------------------------|
| url                          | String  | 是   | -     | JDBC 连接 URL                                  |
| driver                       | String  | 是   | -     | JDBC 驱动类名                                  |
| user                         | String  | 否   | -     | 数据库用户名                                    |
| password                     | String  | 否   | -     | 数据库密码                                      |
| query                        | String  | 否   | -     | 执行的 SQL 查询                                |
| database                     | String  | 否   | main  | 目标数据库名称                                  |
| table                        | String  | 否   | -     | 目标表名称                                      |
| schema                       | String  | 否   | main  | 目标模式名称                                    |
| connection_check_timeout_sec | Int     | 否   | 30    | 连接验证超时时间                                |
| partition_column             | String  | 否   | -     | 数据分区列                                      |
| partition_num                | Int     | 否   | 1     | 分区数量                                        |
| partition_lower_bound        | Long    | 否   | -     | 分区下界                                        |
| partition_upper_bound        | Long    | 否   | -     | 分区上界                                        |
| fetch_size                   | Int     | 否   | 1000  | 每批次获取的行数                                |
| connection_pool_size         | Int     | 否   | 1     | 连接池最大连接数                                |
| connection_pool_timeout      | Int     | 否   | 30000 | 连接池超时时间（毫秒）                          |
| query_timeout                | Int     | 否   | 300   | 查询执行超时时间（秒）                          |
| common-options               |         | 否   | -     | 通用源连接器选项                                |

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
- **安全性**：建议使用环境变量或外部配置管理敏感数据

### url [String]

DuckDB 数据库的 JDBC 连接 URL。

- **必填**：是
- **格式**：
  - 文件数据库：`jdbc:duckdb:/path/to/database.db`
  - 内存数据库：`jdbc:duckdb:`
  - 只读文件：`jdbc:duckdb:/path/to/database.db?access_mode=read_only`
- **示例**：
  - `jdbc:duckdb:/tmp/analytics.db`
  - `jdbc:duckdb:` （内存模式）
  - `jdbc:duckdb:/data/warehouse.db?threads=4`

### query [String]

用于从 DuckDB 提取数据的 SQL SELECT 语句。

- **必填**：否（与 `table` 互斥）
- **支持**：复杂 SQL，包括 JOIN、CTE、窗口函数
- **性能提示**：使用列投影可提高性能
- **示例**：`SELECT id, name, created_at FROM users WHERE created_at >= '2023-01-01'`

### database [String]

DuckDB 中的目标数据库名称。

- **必填**：否
- **默认值**：`main`
- **注意**：DuckDB 使用 'main' 作为默认数据库名称

### table [String]

要读取的目标表名称。

- **必填**：否（与 `query` 互斥）
- **格式**：简单表名或 schema.table
- **示例**：`users` 或 `analytics.user_events`

### schema [String]

DuckDB 中的目标模式名称。

- **必填**：否
- **默认值**：`main`
- **注意**：当 `table` 指定时未包含模式前缀时使用

### connection_check_timeout_sec [Int]

数据库连接验证超时时间。

- **必填**：否
- **默认值**：30 秒
- **范围**：1-300 秒
- **性能**：较低值提供更快的故障检测

### partition_column [String]

通过分区进行并行数据读取的列名。

- **必填**：否
- **支持类型**：数值、日期、时间戳列
- **性能**：为大型数据集启用并行处理
- **最佳实践**：使用分布均匀的列

### partition_num [Int]

并行数据读取的分区数量。

- **必填**：否（仅当指定 partition_column 时）
- **默认值**：1
- **范围**：1-1000
- **性能**：不应超过可用 CPU 核心数
- **计算**：通常设置为 CPU 核心数 × 2

### partition_lower_bound [Long]

第一个分区的最小值。

- **必填**：否（仅当指定 partition_column 时）
- **注意**：用于数值分区列
- **性能**：应代表数据中的实际最小值

### partition_upper_bound [Long]

最后一个分区的最大值。

- **必填**：否（仅当指定 partition_column 时）
- **注意**：用于数值分区列
- **性能**：应代表数据中的实际最大值

### fetch_size [Int]

每次数据库往返检索的行数。

- **必填**：否
- **默认值**：1000
- **范围**：100-50000
- **性能**：
  - 较大值减少网络开销
  - 较小值减少内存使用
  - 最佳范围：大多数情况下 1000-10000

### connection_pool_size [Int]

连接池中的最大数据库连接数。

- **必填**：否
- **默认值**：1
- **范围**：1-100
- **注意**：DuckDB 支持有限的并发连接
- **最佳实践**：对文件数据库保持较低值（1-5）

### connection_pool_timeout [Int]

从连接池获取连接的超时时间。

- **必填**：否
- **默认值**：30000 毫秒
- **范围**：1000-300000
- **性能**：在高并发场景下增加此值

### query_timeout [Int]

查询执行允许的最大时间。

- **必填**：否
- **默认值**：300 秒
- **范围**：10-3600 秒
- **性能**：根据预期查询复杂度设置

### common options

源插件通用参数，详情请参考 [Source Common Options](common-options.md)。

## 配置示例

### 基础配置

```hocon
source {
  DuckDB {
    url = "jdbc:duckdb:/tmp/analytics.db"
    driver = "org.duckdb.DuckDBDriver"
    table = "user_events"
  }
}
```

### 自定义查询配置

```hocon
source {
  DuckDB {
    url = "jdbc:duckdb:/data/warehouse.db"
    driver = "org.duckdb.DuckDBDriver"
    query = """
      SELECT 
        user_id,
        event_type,
        event_timestamp,
        properties
      FROM user_events 
      WHERE event_timestamp >= '2023-01-01'
        AND event_type IN ('purchase', 'signup')
      ORDER BY event_timestamp
    """
  }
}
```

### 并行处理配置

```hocon
source {
  DuckDB {
    url = "jdbc:duckdb:/data/large_dataset.db"
    driver = "org.duckdb.DuckDBDriver"
    table = "transactions"
    partition_column = "transaction_id"
    partition_num = 4
    partition_lower_bound = 1
    partition_upper_bound = 1000000
    fetch_size = 5000
  }
}
```

### 内存数据库配置

```hocon
source {
  DuckDB {
    url = "jdbc:duckdb:"
    driver = "org.duckdb.DuckDBDriver"
    query = "SELECT * FROM read_csv_auto('/tmp/data.csv')"
  }
}
```

### 高性能配置

```hocon
source {
  DuckDB {
    url = "jdbc:duckdb:/data/analytics.db?threads=8&memory_limit=8GB"
    driver = "org.duckdb.DuckDBDriver"
    table = "large_table"
    fetch_size = 10000
    query_timeout = 600
    connection_pool_size = 2
  }
}
```

## 性能调优

### 内存优化
- 在连接 URL 中设置适当的 `memory_limit`
- 根据可用内存使用 `fetch_size`
- 考虑行式与列式数据访问模式

### 查询优化
- 使用列投影最小化数据传输
- 利用 DuckDB 的查询优化器和适当的索引
- 为时间序列数据使用分区修剪

### 并行性配置
- 设置 `partition_num` 匹配 CPU 核心数
- 选择分布良好的分区列
- 平衡分区大小与开销

### 连接管理
- 对文件数据库保持较低的 `connection_pool_size`
- 为复杂查询使用适当的 `query_timeout`
- 监控连接池指标

## 安全考虑

### 文件系统安全
- 确保数据库文件具有适当的文件权限
- 使用安全的文件路径，避免全局可读位置
- 实施备份和恢复程序

### 网络安全
- 使用远程文件时，确保安全协议（SFTP、HTTPS）
- 验证输入路径以防止目录遍历攻击
- 对敏感数据使用加密存储

### 访问控制
- 实施应用级访问控制
- 尽可能使用只读连接
- 审计数据访问模式

## 故障排除

### 常见问题

**连接失败：**
```
错误：数据库文件被锁定
解决方案：确保没有其他进程正在访问数据库文件
```

**内存不足：**
```
错误：内存不足
解决方案：减少 fetch_size 或增加可用内存
```

**查询超时：**
```
错误：查询执行超时
解决方案：增加 query_timeout 或优化查询性能
```

**文件未找到：**
```
错误：找不到数据库文件
解决方案：验证文件路径和权限
```

### 调试技巧

1. **启用详细日志记录：**
   ```hocon
   env {
     job.mode = "BATCH"
     # 启用调试日志
     "seatunnel.logs.level" = "DEBUG"
   }
   ```

2. **单独测试连接：**
   ```sql
   -- 测试基本连接
   SELECT 1 as test_connection;
   ```

3. **监控性能：**
   ```sql
   -- 检查查询执行计划
   EXPLAIN SELECT * FROM your_table;
   ```

4. **验证数据类型：**
   ```sql
   -- 检查表结构
   DESCRIBE your_table;
   ```

## 最佳实践

1. **数据分区**：为大型数据集使用适当的分区策略
2. **内存管理**：根据可用资源设置内存限制
3. **查询优化**：利用 DuckDB 的列式存储优势
4. **错误处理**：实施健壮的重试和回退机制
5. **监控**：跟踪性能指标和查询执行时间
6. **测试**：在负载下验证数据一致性和性能

## 限制

- **并发访问**：文件数据库的并发写入访问有限
- **流处理**：不支持原生流处理（仅批处理）
- **分布式**：单节点处理（无分布式查询执行）
- **模式演化**：动态模式修改支持有限

## 更新日志

### 2.3.0-beta (2023-03-15)
- DuckDB Source 连接器首次发布
- 支持基础数据类型映射
- 基于文件的数据库连接

### 2.3.1+ (2023-04+)
- 持续的稳定性改进和错误修复
- 增强错误处理
- 性能优化

*注意：详细发布说明请参考 [SeaTunnel 官方发布页面](https://github.com/apache/seatunnel/releases)* 