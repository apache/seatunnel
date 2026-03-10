---
title: DataSource SPI
weight: 6
---

# DataSource SPI

## Overview

The DataSource SPI (Service Provider Interface) is an extension mechanism introduced in SeaTunnel for centralized management of data source connection configurations. It allows external metadata systems (such as Apache Gravitino, DataHub, Atlas) to manage data source metadata, while SeaTunnel jobs reference these configurations via a simple `datasource_id`.

### Benefits

- **Simplified Configuration**: Data source connection details (URL, username, password, etc.) are managed externally instead of being duplicated across job configs
- **Enhanced Security**: Sensitive credentials are no longer stored in job configuration files
- **Centralized Management**: Changes to data source configurations only need to be made once in the external system
- **Backward Compatible**: Existing jobs without `datasource_id` continue to work as before

## datasource_id Parameter

`datasource_id` is a common parameter available to all SeaTunnel connectors. When specified, the connector retrieves connection configuration from the external metadata service instead of using direct configuration.

### Usage Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    datasource_id = "mysql-source-01"
    database = "test_db"
    table = "users"
    query = "select * from users where status = 'active'"
  }
}

sink {
  Jdbc {
    datasource_id = "mysql-sink-01"
    database = "reporting_db"
    table = "user_summary"
  }
}
```

When `datasource_id` is specified, the connector will:
1. Use the `datasource_id` to fetch connection details from the external metadata service
2. Merge the fetched configuration with any additional parameters in the job config
3. Job-level parameters take precedence over fetched configuration

## DataSource SPI Interfaces

### DataSourceProvider Interface

The `DataSourceProvider` interface is the entry point for integrating external metadata systems with SeaTunnel. It is discovered via Java SPI using the `@AutoService` annotation.

**Location**: `seatunnel-api/src/main/java/org/apache/seatunnel/api/datasource/DataSourceProvider.java`

```java
public interface DataSourceProvider extends AutoCloseable {

    /**
     * Returns a unique identifier for this provider.
     * Must match the "kind" value in seatunnel.yaml configuration.
     * Examples: "gravitino", "datahub", "atlas","custom"
     */
    String kind();

    /**
     * Initializes the provider with configuration from seatunnel.yaml.
     * Called once during SeaTunnel startup.
     *
     * @param config Provider-specific configuration
     */
    void init(Config config);

    /**
     * Returns the collection of data source mappers supported by this provider.
     * Each mapper handles a specific connector type (Jdbc, Kafka, etc.)
     */
    Collection<DataSourceMapper> dataSourceMappers();

    /**
     * Closes resources held by this provider.
     * Called once during SeaTunnel shutdown.
     */
    @Override
    void close();
}
```

#### Lifecycle

1. **Discovery**: Provider instances are discovered via `@AutoService(DataSourceProvider.class)` and cached
2. **Initialization**: `init(Config)` is called with configuration from `seatunnel.yaml`
3. **Usage**: `dataSourceMappers()` is called to get mappers for resolving `datasource_id`
4. **Cleanup**: `close()` is called during shutdown

#### Resource Management

Providers are responsible for managing all resources needed by their mappers:
- HTTP clients for REST API calls
- Connection pools for database access
- Any other shared resources

Mappers should receive resources from the provider via constructor and not hold resources directly.

### DataSourceMapper Interface

The `DataSourceMapper` interface converts external metadata into SeaTunnel connector configuration.

**Location**: `seatunnel-api/src/main/java/org/apache/seatunnel/api/datasource/DataSourceMapper.java`

```java
public interface DataSourceMapper {

    /**
     * Returns the connector identifier this mapper supports.
     * Must match the SeaTunnel connector's plugin identifier.
     * Examples: "Jdbc", "Kafka", "MySQL-CDC"
     */
    String connectorIdentifier();

    /**
     * Maps a datasource_id to connector configuration.
     *
     * @param datasourceId The data source ID in the external system
     * @return Configuration map for the connector, or null if mapping fails
     */
    Map<String, Object> map(String datasourceId);
}
```

#### Implementation Guidelines

- Mappers should be lightweight and stateless
- Receive resources from the parent `DataSourceProvider` via constructor
- Must be thread-safe as they may be called concurrently
- Handle errors gracefully and return meaningful error messages

## DataSource Configuration

### seatunnel.yaml Configuration

To enable the DataSource Center, add the following configuration to `seatunnel.yaml`:

```yaml
seatunnel:
  engine:
    datasource:
      enabled: true
      kind: gravitino
      gravitino:
        uri: http://127.0.0.1:8090
        metalake: test_metalake
```

### Configuration Options

| Option               | Type    | Default     | Description                                            |
|----------------------|---------|-------------|--------------------------------------------------------|
| `enabled`            | Boolean | `false`     | Whether to enable DataSource Center                    |
| `kind`               | String  | `gravitino` | The DataSource provider type to use                    |
| `gravitino.uri`      | String  | -           | Gravitino server URI (required when kind=gravitino)    |
| `gravitino.metalake` | String  | -           | Gravitino metalake name (required when kind=gravitino) |

## Gravitino Implementation

Apache Gravitino is the default (reference) implementation of the DataSource SPI.

### Overview

Gravitino is a unified metadata catalog for data and AI. The SeaTunnel Gravitino integration provides:
- Centralized JDBC data source management
- Secure credential storage
- Type mapping between Gravitino and SeaTunnel

### GravitinoDataSourceProvider

**Location**: `seatunnel-api/src/main/java/org/apache/seatunnel/api/datasource/gravitino/GravitinoDataSourceProvider.java`

The Gravitino provider implements the `DataSourceProvider` interface:

```java
@AutoService(DataSourceProvider.class)
public class GravitinoDataSourceProvider implements DataSourceProvider {

    @Override
    public String kind() {
        return "gravitino";
    }

    @Override
    public void init(Config config) {
        // Validates and stores URI and metalake configuration
        // Initializes HTTP client for Gravitino API calls
    }

    @Override
    public Collection<DataSourceMapper> dataSourceMappers() {
        // Returns a list of supported mappers
        // Currently only supports JDBC connector
        return Collections.singletonList(
            new GravitinoJdbcDataSourceMapper(buildMetalakeUrl(), client));
    }
}
```

### GravitinoJdbcDataSourceMapper

**Location**: `seatunnel-api/src/main/java/org/apache/seatunnel/api/datasource/gravitino/GravitinoJdbcDataSourceMapper.java`

The JDBC mapper converts Gravitino catalog properties to SeaTunnel JDBC connector configuration.

#### Property Mapping

| Gravitino Property | SeaTunnel Property |
|--------------------|--------------------|
| `jdbc-url`         | `url`              |
| `jdbc-user`        | `username`         |
| `jdbc-password`    | `password`         |
| `jdbc-driver`      | `driver`           |

#### Gravitino Response Example

```json
{
  "code": 0,
  "catalog": {
    "name": "mysql-catalog",
    "type": "relational",
    "provider": "jdbc-mysql",
    "properties": {
      "jdbc-url": "jdbc:mysql://localhost:3306/",
      "jdbc-user": "root",
      "jdbc-password": "secret",
      "jdbc-driver": "com.mysql.cj.jdbc.Driver"
    }
  }
}
```

#### Mapped SeaTunnel Configuration

```hocon
{
  url = "jdbc:mysql://localhost:3306/"
  username = "root"
  password = "secret"
  driver = "com.mysql.cj.jdbc.Driver"
}
```

## Runtime Flow

1. **SeaTunnel Startup**
   - Loads the configured `DataSourceProvider` based on `seatunnel.yaml`
   - Calls `init()` with provider-specific configuration

2. **Job Submission**
   - Parses job configuration
   - Detects presence of `datasource_id` in connector configs

3. **Mapper Resolution**
   - Finds the matching `DataSourceMapper` based on connector identifier (e.g., "Jdbc")
   - Each connector type has its own mapper

4. **Configuration Fetching**
   - Calls `mapper.map(datasourceId)` to retrieve configuration from external system
   - The mapper queries the metadata service and returns connector configuration

5. **Configuration Merge**
   - Merges fetched configuration with job-level parameters
   - Job-level parameters take precedence

## Implementing a Custom Provider

To implement a custom DataSource Provider:

1. **Create a Provider Class**
   ```java
   @AutoService(DataSourceProvider.class)
   public class MyDataSourceProvider implements DataSourceProvider {
       @Override
       public String kind() {
           return "my-provider";
       }

       @Override
       public void init(Config config) {
           // Initialize your client, connection pool, etc.
       }

       @Override
       public Collection<DataSourceMapper> dataSourceMappers() {
           return Arrays.asList(new MyJdbcMapper(), new MyKafkaMapper());
       }

       @Override
       public void close() {
           // Clean up resources
       }
   }
   ```

2. **Create Mapper Classes**
   ```java
   public class MyJdbcMapper implements DataSourceMapper {
       @Override
       public String connectorIdentifier() {
           return "Jdbc";
       }

       @Override
       public Map<String, Object> map(String datasourceId) {
           // Fetch from your metadata service
           // Return SeaTunnel-compatible configuration
       }
   }
   ```

3. **Configure seatunnel.yaml**
   ```yaml
   seatunnel:
     engine:
       datasource:
         enabled: true
         kind: my-provider
         my-provider:
           # provider-specific options
   ```

4. **Package and Deploy**
   - Include your implementation in SeaTunnel's classpath
   - The `@AutoService` annotation will register it automatically
