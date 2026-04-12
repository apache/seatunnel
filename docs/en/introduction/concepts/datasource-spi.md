---
title: DataSource SPI
weight: 6
---

# DataSource SPI

## Overview

The DataSource SPI (Service Provider Interface) is an extension mechanism introduced in SeaTunnel for centralized management of data source connection configurations. It allows external metadata systems to manage data source metadata, while SeaTunnel jobs reference these configurations via a simple `datasource_id`.

### Benefits

- **Simplified Configuration**: Data source connection details (URL, username, password, etc.) are managed externally instead of being duplicated across job configs
- **Enhanced Security**: Sensitive credentials are no longer stored in job configuration files
- **Centralized Management**: Changes to data source configurations only need to be made once in the external system
- **Backward Compatible**: Existing jobs without `datasource_id` continue to work as before
- **Extensible**: Custom metadata systems can be integrated by implementing the `DataSourceProvider` interface

## Using datasource_id

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

## DataSource SPI Specification

This section defines the standard SPI interfaces that all DataSource providers must implement.

### DataSourceProvider Interface

The `DataSourceProvider` interface is the contract for integrating external metadata systems with SeaTunnel. Implementations are discovered via Java SPI using the `@AutoService` annotation.

**Location**: `seatunnel-api/src/main/java/org/apache/seatunnel/api/datasource/DataSourceProvider.java`

```java
public interface DataSourceProvider extends AutoCloseable {

    /**
     * Returns a unique identifier for this provider.
     * Must match the "kind" value in seatunnel.yaml configuration.
     * Examples: "gravitino", "datahub", "atlas", "custom"
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
     * Maps a datasource_id to connector configuration.
     *
     * @param connectorIdentifier The connector identifier (e.g., "Jdbc", "Kafka")
     * @param datasourceId The data source ID in the external system
     * @return Configuration map for the connector, or null if mapping fails
     */
    Map<String, Object> datasourceMap(String connectorIdentifier, String datasourceId);

    /**
     * Closes resources held by this provider.
     * Called once during SeaTunnel shutdown.
     */
    @Override
    void close();
}
```

### Lifecycle

1. **Discovery**: Provider instances are discovered via `@AutoService(DataSourceProvider.class)` and cached
2. **Initialization**: `init(Config)` is called with configuration from `seatunnel.yaml`
3. **Usage**: `datasourceMap(String, String)` is called to resolve `datasource_id` for each connector
4. **Cleanup**: `close()` is called during shutdown

### Resource Management

Providers are responsible for managing all resources needed for datasource mapping:
- HTTP clients for REST API calls
- Connection pools for database access
- Any other shared resources

These resources should be created in `init()`, reused across `datasourceMap()` calls, and cleaned up in `close()`.

## Configuration

The following configuration examples use **Gravitino as the default provider**. For other providers, adjust the `kind` and provider-specific options accordingly.

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

## Default Implementation: Gravitino

Apache Gravitino is the default implementation of the DataSource SPI. To use Gravitino as the DataSource Center, you must set `datasource.enabled` to `true` and explicitly specify `kind` as `gravitino` along with the required configuration parameters.

### datasource_id Configuration

When using Gravitino as the DataSource Center, the `datasource_id` value should be configured as the **catalog name** in Gravitino.

For example, if you have a catalog named `mysql-catalog` in Gravitino, use it directly as the `datasource_id`:

```hocon
source {
  Jdbc {
    datasource_id = "mysql-catalog"
    database = "test_db"
    table = "users"
  }
}
```

### Property Mapping

The Gravitino provider performs **limited property name mapping** from Gravitino catalog properties to SeaTunnel connector configuration. **Only the following four property mappings are supported**:

| Gravitino Property | SeaTunnel Property |
|--------------------|--------------------|
| `jdbc-url`         | `url`              |
| `jdbc-user`        | `username`         |
| `jdbc-password`    | `password`         |
| `jdbc-driver`      | `driver`           |

> **Note**: Any other properties in the Gravitino catalog are not passed. If you need additional property mappings, consider implementing a custom `DataSourceProvider`.

### Connector Support

The Gravitino provider currently supports:
- **Jdbc** connector (fully supported)

### Example

#### Gravitino Catalog Response

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

## Implementing a Custom Provider

To integrate a custom metadata system with SeaTunnel, implement the `DataSourceProvider` interface.

### Step 1: Add Dependency

Add the `seatunnel-api` dependency to your project's `pom.xml`:

```xml
<dependency>
    <groupId>org.apache.seatunnel</groupId>
    <artifactId>seatunnel-api</artifactId>
    <version>${seatunnel.version}</version>
    <scope>provided</scope>
</dependency>
```

> **Note**: Use `<scope>provided</scope>` since SeaTunnel already includes this dependency at runtime.

### Step 2: Create a Provider Class

```java
@AutoService(DataSourceProvider.class)
public class MyDataSourceProvider implements DataSourceProvider {

    private HttpClient httpClient;

    @Override
    public String kind() {
        return "my-provider";
    }

    @Override
    public void init(Config config) {
        // Initialize your client, connection pool, etc.
        this.httpClient = HttpClient.newBuilder().build();
    }

    @Override
    public Map<String, Object> datasourceMap(String connectorIdentifier, String datasourceId) {
        // Fetch from your metadata service based on connector type
        // Return SeaTunnel-compatible configuration
        switch (connectorIdentifier.toLowerCase()) {
            case "jdbc":
                return fetchJdbcConfig(datasourceId);
            case "kafka":
                return fetchKafkaConfig(datasourceId);
            default:
                return Collections.emptyMap();
        }
    }

    @Override
    public void close() {
        // Clean up resources
        if (httpClient != null) {
            // Clean up HTTP client
        }
    }
}
```

### Step 3: Configure seatunnel.yaml

```yaml
seatunnel:
  engine:
    datasource:
      enabled: true
      kind: my-provider
      my-provider:
        endpoint: https://my-metadata-service.com
        api-key: your-api-key
```

### Step 4: Package and Deploy

- Include your implementation in SeaTunnel's classpath
- The `@AutoService` annotation will register it automatically via Java SPI

## Runtime Flow

1. **SeaTunnel Startup**
   - Loads the configured `DataSourceProvider` based on `seatunnel.yaml`
   - Calls `init()` with provider-specific configuration

2. **Job Submission**
   - Parses job configuration
   - Detects presence of `datasource_id` in connector configs

3. **Configuration Fetching**
   - Calls `provider.datasourceMap(connectorIdentifier, datasourceId)` to retrieve configuration from external system
   - The provider queries the metadata service and returns connector configuration

4. **Configuration Merge**
   - Merges fetched configuration with job-level parameters
   - Job-level parameters take precedence
