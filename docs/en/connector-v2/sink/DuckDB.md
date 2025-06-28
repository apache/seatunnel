# DuckDB Sink

> DuckDB Sink connector

## Description

The DuckDB Sink connector writes data to DuckDB databases with high performance and reliability. DuckDB is an in-process SQL OLAP database management system optimized for analytical workloads. This connector supports both file-based and in-memory DuckDB databases, providing efficient data ingestion for analytics pipelines, data warehousing, and real-time processing scenarios.

**Key Characteristics:**
- High-performance columnar storage with excellent compression
- ACID-compliant transactions with rollback support
- Automatic table creation with schema inference
- Optimized bulk loading for large datasets
- Support for complex data types and nested structures

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [upsert](../../concept/connector-v2-features.md)
- [x] [cdc](../../concept/connector-v2-features.md)
- [x] [support multiple table writing](../../concept/connector-v2-features.md)

## Data Type Mapping

SeaTunnel data types are automatically mapped to DuckDB data types as follows:

| SeaTunnel Data Type            | DuckDB Data Type               | Notes                                    |
|--------------------------------|--------------------------------|------------------------------------------|
| BOOLEAN                        | BOOLEAN                        |                                          |
| TINYINT                        | TINYINT                        |                                          |
| SMALLINT                       | SMALLINT                       |                                          |
| INT                            | INTEGER                        |                                          |
| BIGINT                         | BIGINT                         |                                          |
| FLOAT                          | FLOAT                          |                                          |
| DOUBLE                         | DOUBLE                         |                                          |
| DECIMAL(p,s)                   | DECIMAL(p,s)                   | Precision and scale preserved            |
| STRING                         | VARCHAR                        | Length automatically determined          |
| BYTES                          | BLOB                           |                                          |
| DATE                           | DATE                           |                                          |
| TIME                           | TIME                           |                                          |
| TIMESTAMP                      | TIMESTAMP                      |                                          |
| ARRAY&lt;T&gt;                     | T[]                            | Nested arrays supported                  |
| ROW                            | STRUCT                         | Named fields preserved                   |
| MAP&lt;K,V&gt;                     | MAP(K,V)                       | Key-value mapping preserved              |

## Options

| name                         | type    | required | default value | description                                    |
|------------------------------|---------|----------|---------------|------------------------------------------------|
| url                          | String  | Yes      | -             | JDBC connection URL                            |
| driver                       | String  | Yes      | -             | JDBC driver class name                         |
| user                         | String  | No       | -             | Database username                              |
| password                     | String  | No       | -             | Database password                              |
| database                     | String  | No       | main          | Target database name                           |
| table                        | String  | Yes      | -             | Target table name                              |
| schema                       | String  | No       | main          | Target schema name                             |
| connection_check_timeout_sec | Int     | No       | 30            | Connection validation timeout                  |
| batch_size                   | Int     | No       | 1000          | Batch size for bulk operations                 |
| primary_keys                 | Array   | No       | -             | Primary key columns for upsert                 |
| max_retries                  | Int     | No       | 3             | Maximum retry attempts                         |
| retry_backoff_multiplier_ms  | Int     | No       | 1000          | Retry backoff multiplier                       |
| max_retry_backoff_ms         | Int     | No       | 10000         | Maximum retry backoff time                     |
| is_exactly_once              | Boolean | No       | false         | Enable exactly-once processing                 |
| generate_sink_sql            | Boolean | No       | false         | Auto-generate table schema                     |
| xa_data_source_class_name    | String  | No       | -             | XA DataSource class for transactions           |
| max_commit_attempts          | Int     | No       | 3             | Maximum commit retry attempts                  |
| transaction_timeout_sec      | Int     | No       | 300           | Transaction timeout                            |
| connection_pool_size         | Int     | No       | 1             | Maximum connections in pool                    |
| enable_upsert                | Boolean | No       | false         | Enable upsert mode                             |
| save_mode                    | Enum    | No       | append        | Data save mode                                 |
| common-options               |         | No       | -             | Common sink connector options                  |

### driver [String]

The JDBC driver class name for connecting to DuckDB.

- **Required**: Yes
- **Value**: `org.duckdb.DuckDBDriver`
- **Note**: Ensure DuckDB JDBC driver is available in the classpath

### user [String]

Username for DuckDB authentication.

- **Required**: No
- **Default**: Empty (no authentication required)
- **Note**: DuckDB typically doesn't require authentication for file-based databases

### password [String]

Password for DuckDB authentication.

- **Required**: No
- **Default**: Empty
- **Security**: Use environment variables or secure configuration stores

### url [String]

The JDBC connection URL for DuckDB database.

- **Required**: Yes
- **Format**:
  - File database: `jdbc:duckdb:/path/to/database.db`
  - In-memory database: `jdbc:duckdb:`
  - Read-write mode: `jdbc:duckdb:/path/to/database.db?access_mode=read_write`
- **Performance Parameters**:
  - `threads=N`: Set number of worker threads
  - `memory_limit=XGB`: Set memory limit
  - `max_memory=XGB`: Set maximum memory usage
- **Examples**:
  - `jdbc:duckdb:/data/warehouse.db`
  - `jdbc:duckdb:/tmp/analytics.db?threads=4&memory_limit=2GB`

### database [String]

The target database name in DuckDB.

- **Required**: No
- **Default**: `main`
- **Note**: DuckDB uses 'main' as the default database name

### table [String]

The target table name for data writing.

- **Required**: Yes
- **Format**: Simple table name or schema.table
- **Auto-creation**: Enabled when `generate_sink_sql` is true
- **Example**: `user_events` or `analytics.user_events`

### schema [String]

The target schema name in DuckDB.

- **Required**: No
- **Default**: `main`
- **Note**: Used when table is specified without schema prefix

### connection_check_timeout_sec [Int]

Timeout for database connection validation.

- **Required**: No
- **Default**: 30 seconds
- **Range**: 1-300 seconds
- **Performance**: Lower values provide faster failure detection

### batch_size [Int]

Number of rows to insert in each batch operation.

- **Required**: No
- **Default**: 1000
- **Range**: 100-50000
- **Performance**:
  - Larger batches improve throughput
  - Smaller batches reduce memory usage
  - Optimal range: 1000-10000 for most cases

### primary_keys [Array]

Column names that form the primary key for upsert operations.

- **Required**: No (required for upsert mode)
- **Format**: Array of column names
- **Usage**: Enables UPSERT (INSERT OR REPLACE) operations
- **Example**: `["id"]` or `["user_id", "event_date"]`

### max_retries [Int]

Maximum number of retry attempts for failed operations.

- **Required**: No
- **Default**: 3
- **Range**: 0-10
- **Behavior**: Exponential backoff between retries

### retry_backoff_multiplier_ms [Int]

Base delay multiplier for retry backoff strategy.

- **Required**: No
- **Default**: 1000 milliseconds
- **Range**: 100-10000
- **Calculation**: delay = multiplier × (2 ^ attempt_number)

### max_retry_backoff_ms [Int]

Maximum delay between retry attempts.

- **Required**: No
- **Default**: 10000 milliseconds
- **Range**: 1000-300000
- **Purpose**: Prevents excessive delay in retry loops

### is_exactly_once [Boolean]

Enable exactly-once processing semantics using XA transactions.

- **Required**: No
- **Default**: false
- **Impact**: Ensures data consistency but may affect performance
- **Note**: Requires XA-compliant configuration

### generate_sink_sql [Boolean]

Automatically generate table creation SQL based on source schema.

- **Required**: No
- **Default**: false
- **Behavior**: Creates table if it doesn't exist
- **Schema Detection**: Infers data types from source data

### xa_data_source_class_name [String]

XA DataSource class name for distributed transactions.

- **Required**: No (required when is_exactly_once=true)
- **Value**: Typically `org.duckdb.DuckDBDataSource`
- **Usage**: Enables two-phase commit protocol

### max_commit_attempts [Int]

Maximum attempts for transaction commit operations.

- **Required**: No
- **Default**: 3
- **Range**: 1-10
- **Usage**: Provides resilience against temporary commit failures

### transaction_timeout_sec [Int]

Maximum time allowed for transaction completion.

- **Required**: No
- **Default**: 300 seconds
- **Range**: 10-3600 seconds
- **Impact**: Prevents long-running transaction locks

### connection_pool_size [Int]

Maximum number of database connections in the pool.

- **Required**: No
- **Default**: 1
- **Range**: 1-100
- **Note**: DuckDB supports limited concurrent connections
- **Best Practice**: Keep low (1-5) for file databases

### enable_upsert [Boolean]

Enable upsert (INSERT OR REPLACE) operations.

- **Required**: No
- **Default**: false
- **Dependency**: Requires `primary_keys` configuration
- **Performance**: May be slower than INSERT for large datasets

### save_mode [Enum]

Data saving strategy for the target table.

- **Required**: No
- **Default**: `append`
- **Values**:
  - `append`: Insert new data (default)
  - `overwrite`: Truncate table before insert
  - `error_if_exists`: Fail if table already exists
  - `ignore_if_exists`: Skip if table already exists

### common options

Sink plugin common parameters, please refer to [Sink Common Options](common-options.md) for details.

## Examples

### Basic Configuration

```hocon
sink {
  DuckDB {
    url = "jdbc:duckdb:/tmp/analytics.db"
    driver = "org.duckdb.DuckDBDriver"
    table = "user_events"
    generate_sink_sql = true
  }
}
```

### High-Performance Bulk Loading

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

### Upsert Configuration

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

### Exactly-Once Processing

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

### Multi-Schema Configuration

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

### In-Memory High-Speed Processing

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

## Transaction Management

### ACID Compliance

DuckDB provides full ACID compliance with the following guarantees:

- **Atomicity**: All operations in a transaction succeed or fail together
- **Consistency**: Database remains in a valid state after transactions
- **Isolation**: Concurrent transactions don't interfere with each other
- **Durability**: Committed transactions persist through system failures

### Transaction Modes

1. **Auto-commit Mode** (Default):
   - Each batch is automatically committed
   - Fastest performance for bulk operations
   - Limited rollback capabilities

2. **Exactly-Once Mode**:
   - Uses XA transactions for guarantees
   - Enables cross-system consistency
   - Slightly reduced performance

3. **Batch Transaction Mode**:
   - Groups multiple batches in transactions
   - Balances performance and consistency
   - Configurable transaction boundaries

## Performance Tuning

### Memory Optimization
- Set appropriate `memory_limit` in connection URL
- Use `batch_size` based on available memory and row size
- Monitor memory usage during large data loads

### Bulk Loading Optimization
- Use larger `batch_size` for better throughput (5000-20000)
- Disable unnecessary indexes during bulk loading
- Consider `save_mode=overwrite` for full table refresh

### Connection Management
- Keep `connection_pool_size` low for file databases
- Use appropriate `transaction_timeout_sec` for large operations
- Monitor connection pool metrics and adjust accordingly

### Query Optimization
- Leverage DuckDB's columnar storage advantages
- Use appropriate data types to minimize storage
- Consider partitioning strategies for time-series data

## Security Considerations

### File System Security
- Ensure proper file permissions on database files
- Use secure file paths and avoid world-readable locations
- Implement regular backup and recovery procedures
- Consider encryption at rest for sensitive data

### Access Control
- Implement application-level access controls
- Use principle of least privilege for database operations
- Audit write operations and data modifications
- Validate input data to prevent injection attacks

### Transaction Security
- Use exactly-once mode for critical financial data
- Implement proper error handling and rollback procedures
- Monitor for unusual transaction patterns
- Ensure backup consistency during high-volume operations

## Troubleshooting

### Common Issues

**Table Creation Failures:**
```
Error: Table already exists
Solution: Set save_mode = "append" or "overwrite" as appropriate
```

**Memory Issues:**
```
Error: Out of memory during bulk insert
Solution: Reduce batch_size or increase memory_limit in URL
```

**Transaction Timeouts:**
```
Error: Transaction timeout exceeded
Solution: Increase transaction_timeout_sec or optimize data processing
```

**Connection Pool Exhaustion:**
```
Error: Unable to acquire connection from pool
Solution: Increase connection_pool_size or reduce concurrent operations
```

**Data Type Mismatches:**
```
Error: Cannot convert data type
Solution: Verify data type mapping or enable generate_sink_sql
```

### Debugging Tips

1. **Enable verbose logging:**
   ```hocon
   env {
     job.mode = "BATCH"
     "seatunnel.logs.level" = "DEBUG"
   }
   ```

2. **Test table creation:**
   ```sql
   -- Verify table structure
   DESCRIBE your_table;
   SELECT COUNT(*) FROM your_table;
   ```

3. **Monitor performance:**
   ```sql
   -- Check database statistics
   SELECT * FROM duckdb_tables();
   SELECT * FROM duckdb_columns();
   ```

4. **Validate transactions:**
   ```sql
   -- Check active transactions
   SELECT * FROM duckdb_transactions();
   ```

## Best Practices

1. **Schema Design**: Use appropriate data types and primary keys
2. **Batch Sizing**: Optimize batch_size based on data characteristics
3. **Error Handling**: Implement robust retry and rollback strategies
4. **Monitoring**: Track performance metrics and error rates
5. **Testing**: Validate data integrity and performance under load
6. **Backup**: Maintain regular backup schedules for production data
7. **Resource Management**: Monitor memory and disk usage
8. **Security**: Implement proper access controls and encryption

## Limitations

- **Concurrent Writers**: Limited concurrent write access to file databases
- **Streaming**: No native streaming support (batch processing only)
- **Distributed Transactions**: Limited distributed transaction support
- **Schema Evolution**: Dynamic schema changes may require table recreation
- **Large Objects**: Very large BLOB objects may impact performance

## Advanced Features

### Complex Data Types
```hocon
# Example with nested structures
sink {
  DuckDB {
    url = "jdbc:duckdb:/data/complex.db"
    driver = "org.duckdb.DuckDBDriver"
    table = "nested_data"
    generate_sink_sql = true
    # Supports ARRAY, STRUCT, MAP types automatically
  }
}
```

### Partitioned Loading
```sql
-- DuckDB supports partitioned tables
CREATE TABLE events_partitioned (
  id BIGINT,
  event_date DATE,
  data VARCHAR
) PARTITION BY (event_date);
```

### Compression Optimization
```hocon
# Enable compression for better storage efficiency
sink {
  DuckDB {
    url = "jdbc:duckdb:/data/compressed.db?enable_http_metadata_cache=true"
    driver = "org.duckdb.DuckDBDriver"
    table = "compressed_data"
    # DuckDB automatically applies compression
  }
}
```

## Changelog

### 2.3.0-beta (2023-03-15)
- Initial release of DuckDB Sink Connector
- Support for basic data type mapping
- File-based database connectivity

### 2.3.1+ (2023-04+)
- Ongoing stability improvements and bug fixes
- Enhanced error handling
- Performance optimizations

*Note: For detailed release notes, please refer to the [official SeaTunnel releases](https://github.com/apache/seatunnel/releases)* 