# DuckDB Source

> DuckDB Source connector

## Description

The DuckDB Source connector reads data from DuckDB databases. DuckDB is an in-process SQL OLAP database management system that is particularly suitable for analytical workloads. This connector supports both file-based and in-memory DuckDB databases, making it ideal for data analytics pipelines, data migration, and real-time data processing scenarios.

**Key Characteristics:**
- High-performance columnar storage engine
- ACID compliance with transaction support
- Zero-configuration setup for file-based databases
- Excellent performance for analytical queries
- Support for complex SQL operations including window functions, CTEs, and aggregations

## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [ ] [stream](../../concept/connector-v2-features.md) 
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [column projection](../../concept/connector-v2-features.md)
- [x] [parallelism](../../concept/connector-v2-features.md)
- [x] [support user-defined split](../../concept/connector-v2-features.md)

## Data Type Mapping

DuckDB data types are automatically mapped to SeaTunnel data types as follows:

| DuckDB Data Type               | SeaTunnel Data Type                |
|--------------------------------|------------------------------------|
| BOOLEAN                        | BOOLEAN                            |
| TINYINT                        | TINYINT                            |
| SMALLINT                       | SMALLINT                           |
| INTEGER                        | INT                                |
| BIGINT                         | BIGINT                             |
| FLOAT                          | FLOAT                              |
| DOUBLE                         | DOUBLE                             |
| DECIMAL(p,s)                   | DECIMAL(p,s)                       |
| VARCHAR(n)                     | STRING                             |
| CHAR(n)                        | STRING                             |
| TEXT                           | STRING                             |
| DATE                           | DATE                               |
| TIME                           | TIME                               |
| TIMESTAMP                      | TIMESTAMP                          |
| TIMESTAMP WITH TIME ZONE       | TIMESTAMP                          |
| BLOB                           | BYTES                              |
| ARRAY                          | ARRAY                              |
| STRUCT                         | ROW                                |
| MAP                            | MAP                                |

## Options

| name                         | type    | required | default value | description                                    |
|------------------------------|---------|----------|---------------|------------------------------------------------|
| url                          | String  | Yes      | -             | JDBC connection URL                            |
| driver                       | String  | Yes      | -             | JDBC driver class name                         |
| user                         | String  | No       | -             | Database username                              |
| password                     | String  | No       | -             | Database password                              |
| query                        | String  | No       | -             | SQL query to execute                           |
| database                     | String  | No       | main          | Target database name                           |
| table                        | String  | No       | -             | Target table name                              |
| schema                       | String  | No       | main          | Target schema name                             |
| connection_check_timeout_sec | Int     | No       | 30            | Connection validation timeout                  |
| partition_column             | String  | No       | -             | Column for data partitioning                   |
| partition_num                | Int     | No       | 1             | Number of partitions                           |
| partition_lower_bound        | Long    | No       | -             | Lower bound for partitioning                   |
| partition_upper_bound        | Long    | No       | -             | Upper bound for partitioning                   |
| fetch_size                   | Int     | No       | 1000          | Number of rows to fetch per batch             |
| connection_pool_size         | Int     | No       | 1             | Maximum connections in pool                    |
| connection_pool_timeout      | Int     | No       | 30000         | Connection pool timeout (milliseconds)         |
| query_timeout                | Int     | No       | 300           | Query execution timeout (seconds)              |
| common-options               |         | No       | -             | Common source connector options                |

### driver [String]

The JDBC driver class name for connecting to DuckDB. 

- **Required**: Yes
- **Value**: `org.duckdb.DuckDBDriver`
- **Note**: Ensure DuckDB JDBC driver is available in the classpath

### user [String]

Username for DuckDB authentication. 

- **Required**: No
- **Default**: Empty (no authentication)
- **Note**: DuckDB typically doesn't require authentication for file-based databases

### password [String]

Password for DuckDB authentication.

- **Required**: No  
- **Default**: Empty
- **Security**: Use environment variables or external configuration for sensitive data

### url [String]

The JDBC connection URL for DuckDB database.

- **Required**: Yes
- **Format**: 
  - File database: `jdbc:duckdb:/path/to/database.db`
  - In-memory database: `jdbc:duckdb:`
  - Read-only file: `jdbc:duckdb:/path/to/database.db?access_mode=read_only`
- **Examples**:
  - `jdbc:duckdb:/tmp/analytics.db`
  - `jdbc:duckdb:` (in-memory)
  - `jdbc:duckdb:/data/warehouse.db?threads=4`

### query [String]

SQL SELECT statement to extract data from DuckDB.

- **Required**: No (mutually exclusive with `table`)
- **Supports**: Complex SQL including JOINs, CTEs, window functions
- **Performance Tip**: Use column projection to improve performance
- **Example**: `SELECT id, name, created_at FROM users WHERE created_at >= '2023-01-01'`

### database [String]

The target database name in DuckDB.

- **Required**: No
- **Default**: `main`
- **Note**: DuckDB uses 'main' as the default database name

### table [String]

The target table name to read from.

- **Required**: No (mutually exclusive with `query`)
- **Format**: Simple table name or schema.table
- **Example**: `users` or `analytics.user_events`

### schema [String] 

The target schema name in DuckDB.

- **Required**: No
- **Default**: `main`
- **Note**: Used when `table` is specified without schema prefix

### connection_check_timeout_sec [Int]

Timeout for database connection validation.

- **Required**: No
- **Default**: 30 seconds
- **Range**: 1-300 seconds
- **Performance**: Lower values provide faster failure detection

### partition_column [String]

Column name for parallel data reading through partitioning.

- **Required**: No
- **Supported Types**: Numeric, date, timestamp columns
- **Performance**: Enables parallel processing for large datasets
- **Best Practice**: Use columns with even distribution

### partition_num [Int]

Number of parallel partitions for data reading.

- **Required**: No (only when partition_column is specified)
- **Default**: 1
- **Range**: 1-1000  
- **Performance**: Should not exceed available CPU cores
- **Calculation**: Typically set to number of CPU cores × 2

### partition_lower_bound [Long]

Minimum value for the first partition.

- **Required**: No (only when partition_column is specified)
- **Note**: Used for numeric partition columns
- **Performance**: Should represent actual minimum value in data

### partition_upper_bound [Long] 

Maximum value for the last partition.

- **Required**: No (only when partition_column is specified)
- **Note**: Used for numeric partition columns
- **Performance**: Should represent actual maximum value in data

### fetch_size [Int]

Number of rows retrieved in each database round trip.

- **Required**: No
- **Default**: 1000
- **Range**: 100-50000
- **Performance**: 
  - Larger values reduce network overhead
  - Smaller values reduce memory usage
  - Optimal range: 1000-10000 for most cases

### connection_pool_size [Int]

Maximum number of database connections in the pool.

- **Required**: No
- **Default**: 1
- **Range**: 1-100
- **Note**: DuckDB supports limited concurrent connections
- **Best Practice**: Keep low (1-5) for file databases

### connection_pool_timeout [Int]

Timeout for acquiring connections from the pool.

- **Required**: No
- **Default**: 30000 milliseconds
- **Range**: 1000-300000
- **Performance**: Increase for high-concurrency scenarios

### query_timeout [Int]

Maximum time allowed for query execution.

- **Required**: No
- **Default**: 300 seconds
- **Range**: 10-3600 seconds
- **Performance**: Set based on expected query complexity

### common options

Source plugin common parameters, please refer to [Source Common Options](common-options.md) for details.

## Examples

### Basic Configuration

```hocon
source {
  DuckDB {
    url = "jdbc:duckdb:/tmp/analytics.db"
    driver = "org.duckdb.DuckDBDriver"
    table = "user_events"
  }
}
```

### Custom Query Configuration

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

### Parallel Processing Configuration

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

### In-Memory Database Configuration

```hocon
source {
  DuckDB {
    url = "jdbc:duckdb:"
    driver = "org.duckdb.DuckDBDriver"
    query = "SELECT * FROM read_csv_auto('/tmp/data.csv')"
  }
}
```

### High-Performance Configuration

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

## Performance Tuning

### Memory Optimization
- Set appropriate `memory_limit` in connection URL
- Use `fetch_size` based on available memory
- Consider row-wise vs columnar data access patterns

### Query Optimization  
- Use column projection to minimize data transfer
- Leverage DuckDB's query optimizer with proper indexing
- Utilize partition pruning for time-series data

### Parallelism Configuration
- Set `partition_num` to match CPU cores
- Choose partition columns with good distribution
- Balance partition size vs overhead

### Connection Management
- Keep `connection_pool_size` low for file databases
- Use appropriate `query_timeout` for complex queries
- Monitor connection pool metrics

## Security Considerations

### File System Security
- Ensure proper file permissions on database files
- Use secure file paths and avoid world-readable locations
- Implement backup and recovery procedures

### Network Security  
- When using remote files, ensure secure protocols (SFTP, HTTPS)
- Validate input paths to prevent directory traversal attacks
- Use encrypted storage for sensitive data

### Access Control
- Implement application-level access controls
- Use read-only connections when possible
- Audit data access patterns

## Troubleshooting

### Common Issues

**Connection Failures:**
```
Error: Database file is locked
Solution: Ensure no other processes are accessing the database file
```

**Out of Memory:**
```  
Error: Out of memory
Solution: Reduce fetch_size or increase available memory
```

**Query Timeout:**
```
Error: Query execution timeout
Solution: Increase query_timeout or optimize query performance
```

**File Not Found:**
```
Error: Database file not found
Solution: Verify file path and permissions
```

### Debugging Tips

1. **Enable detailed logging:**
   ```hocon
   env {
     job.mode = "BATCH"
     # Enable debug logging
     "seatunnel.logs.level" = "DEBUG"
   }
   ```

2. **Test connection separately:**
   ```sql
   -- Test basic connectivity
   SELECT 1 as test_connection;
   ```

3. **Monitor performance:**
   ```sql
   -- Check query execution plan
   EXPLAIN SELECT * FROM your_table;
   ```

4. **Validate data types:**
   ```sql
   -- Check table schema
   DESCRIBE your_table;
   ```

## Best Practices

1. **Data Partitioning**: Use appropriate partition strategies for large datasets
2. **Memory Management**: Set memory limits based on available resources  
3. **Query Optimization**: Leverage DuckDB's columnar storage advantages
4. **Error Handling**: Implement robust retry and fallback mechanisms
5. **Monitoring**: Track performance metrics and query execution times
6. **Testing**: Validate data consistency and performance under load

## Limitations

- **Concurrent Access**: Limited concurrent write access to file databases
- **Streaming**: No native streaming support (batch processing only)
- **Distributed**: Single-node processing (no distributed query execution)
- **Schema Evolution**: Limited dynamic schema modification support

## Changelog

### 2.3.0-beta (2023-03-15)
- Initial release of DuckDB Source Connector
- Support for basic data type mapping
- File-based database connectivity

### 2.3.1+ (2023-04+)
- Ongoing stability improvements and bug fixes
- Enhanced error handling
- Performance optimizations

*Note: For detailed release notes, please refer to the [official SeaTunnel releases](https://github.com/apache/seatunnel/releases)* 