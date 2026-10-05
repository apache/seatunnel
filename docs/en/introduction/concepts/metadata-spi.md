---
title: Metadata SPI
weight: 6
---

# Metadata SPI

## Overview

The Metadata SPI (Service Provider Interface) is an extension mechanism introduced in SeaTunnel for centralized management of data source connection configurations and table schema metadata. It allows external metadata systems to manage data source metadata, while SeaTunnel jobs reference these configurations via a simple `metadata_datasource_id`.

### Benefits

- **Simplified Configuration**: Data source connection details (URL, username, password, etc.) are managed externally instead of being duplicated across job configs
- **Enhanced Security**: Sensitive credentials are no longer stored in job configuration files
- **Centralized Management**: Changes to data source configurations only need to be made once in the external system
- **Schema Discovery**: Automatic table schema retrieval from metadata systems
- **Extensible**: Custom metadata systems can be integrated by implementing the `MetadataProvider` interface

### Engine Support

> **Important**: Metadata SPI is currently only supported on the **SeaTunnel Zeta engine**. It is not yet compatible with Flink or Spark engines.

## Using metadata_datasource_id

`metadata_datasource_id` is a common parameter available to all SeaTunnel connectors. When specified, the connector retrieves connection configuration from the external metadata service instead of using direct configuration.

### Usage Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  Jdbc {
    metadata_datasource_id = "mysql-source-01"
    database = "test_db"
    table = "users"
    query = "select * from users where status = 'active'"
  }
}

sink {
  Jdbc {
    metadata_datasource_id = "mysql-sink-01"
    database = "reporting_db"
    table = "user_summary"
  }
}
```

When `metadata_datasource_id` is specified, the connector will:
1. Use the `metadata_datasource_id` to fetch connection details from the external metadata service
2. Merge the fetched configuration with any additional parameters in the job config
3. Job-level parameters take precedence over fetched configuration

## Using metadata_table_id

`metadata_table_id` is a parameter available in the `schema` configuration for connectors that support schema definition. When specified, the connector retrieves table schema from the external metadata service instead of using manual `columns` definition.

### Usage Example

```hocon
env {
  parallelism = 1
  job.mode = "BATCH"
}

source {
  LocalFile {
    path = "/tmp/data"
    file_format_type = "json"
    schema {
      table = "db.users"
      metadata_table_id = "mysql-catalog.test_db.users"
    }
  }
}
```

When `metadata_table_id` is specified in the schema configuration, the connector will:
1. Use the `metadata_table_id` to fetch table schema from the external metadata service
2. The fetched schema includes column definitions, data types, and constraints
3. No need to manually define `columns`

See [Schema Feature](./schema-feature.md) for more information on schema configuration.

## Metadata SPI Specification

This section defines the standard SPI interfaces that all Metadata providers must implement.

### MetadataProvider Interface

The `MetadataProvider` interface is the contract for integrating external metadata systems with SeaTunnel. Implementations are discovered via Java SPI using the `@AutoService` annotation.

**Location**: `seatunnel-api/src/main/java/org/apache/seatunnel/api/metadata/MetadataProvider.java`

```java
public interface MetadataProvider extends AutoCloseable {

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
     * Maps a metadata_datasource_id to connector configuration.
     *
     * @param connectorIdentifier The connector identifier (e.g., "Jdbc", "Kafka")
     * @param metaDataDatasourceId The data source ID in the external system
     * @return Configuration map for the connector, or null if mapping fails
     */
    Map<String, Object> datasourceMap(String connectorIdentifier, String metaDataDatasourceId);

    /**
     * Retrieves the table schema for the given metadata table ID.
     *
     * <p>This method fetches table metadata from the external metadata system, including column
     * definitions, data types, and constraints.
     *
     * @param metaDataTableId the table ID in the external metadata system
     * @return table schema if found, empty otherwise
     */
    Optional<TableSchema> tableSchema(String metaDataTableId);

    /**
     * Closes resources held by this provider.
     * Called once during SeaTunnel shutdown.
     */
    @Override
    void close();
}
```

### Lifecycle

1. **Discovery**: Provider instances are discovered via `@AutoService(MetadataProvider.class)` and cached
2. **Initialization**: `init(Config)` is called with configuration from `seatunnel.yaml`
3. **Usage**: `datasourceMap(String, String)` is called to resolve `metadata_datasource_id` for each connector
4. **Schema Retrieval**: `tableSchema(String)` is called to fetch table schema when needed
5. **Cleanup**: `close()` is called during shutdown

### Resource Management

Providers are responsible for managing all resources needed for datasource mapping:
- HTTP clients for REST API calls
- Connection pools for database access
- Any other shared resources

These resources should be created in `init()`, reused across `datasourceMap()` and `tableSchema()` calls, and cleaned up in `close()`.

## Configuration

The following configuration examples use **Gravitino as the default provider**. For other providers, adjust the `kind` and provider-specific options accordingly.

### seatunnel.yaml Configuration

To enable the Metadata Center, add the following configuration to `seatunnel.yaml`:

```yaml
seatunnel:
  engine:
     metadata:
      enabled: true
      kind: gravitino
      gravitino:
        uri: http://127.0.0.1:8090
        metalake: test_metalake
```

### Configuration Options

| Option               | Type    | Default     | Description                                            |
|----------------------|---------|-------------|--------------------------------------------------------|
| `enabled`            | Boolean | `false`     | Whether to enable Metadata Center                      |
| `kind`               | String  | `gravitino` | The Metadata provider type to use                      |
| `gravitino.uri`      | String  | -           | Gravitino server URI (required when kind=gravitino)    |
| `gravitino.metalake` | String  | -           | Gravitino metalake name (required when kind=gravitino) |

## Default Implementation: Gravitino

Apache Gravitino is the default implementation of the Metadata SPI. To use Gravitino as the Metadata Center, you must set `metadata.enabled` to `true` and explicitly specify `kind` as `gravitino` along with the required configuration parameters.

### metadata_datasource_id Configuration

When using Gravitino as the Metadata Center, the `metadata_datasource_id` value should be configured as the **catalog name** in Gravitino.

For example, if you have a catalog named `mysql-catalog` in Gravitino, use it directly as the `metadata_datasource_id`:

```hocon
source {
  Jdbc {
    metadata_datasource_id = "mysql-catalog"
    database = "test_db"
    table = "users"
  }
}
```

### Table Schema Retrieval

The `tableSchema(String metaDataTableId)` method allows SeaTunnel to automatically fetch table schema from Gravitino. When using this feature, the table schema including column definitions, data types, and constraints is retrieved without manual configuration.

For Gravitino, the `metaDataTableId` should be formatted as `{catalog}.{schema}.{table}`. For example, `mysql-catalog.test_db.users`.

See [Gravitino Type Mapping](./gravitino-type-mapping.md) for details on how Gravitino types are mapped to SeaTunnel types.

### Property Mapping

The Gravitino provider performs **limited property name mapping** from Gravitino catalog properties to SeaTunnel connector configuration. **Only the following four property mappings are supported**:

| Gravitino Property | SeaTunnel Property |
|--------------------|--------------------|
| `jdbc-url`         | `url`              |
| `jdbc-user`        | `username`         |
| `jdbc-password`    | `password`         |
| `jdbc-driver`      | `driver`           |

> **Note**: Any other properties in the Gravitino catalog are not passed. If you need additional property mappings, consider implementing a custom `MetadataProvider`.

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

To integrate a custom metadata system with SeaTunnel, implement the `MetadataProvider` interface.

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
@AutoService(MetadataProvider.class)
public class MyMetadataProvider implements MetadataProvider {

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
    public Map<String, Object> datasourceMap(String connectorIdentifier, String metaDataDatasourceId) {
        // Fetch from your metadata service based on connector type
        // Return SeaTunnel-compatible configuration
        switch (connectorIdentifier.toLowerCase()) {
            case "jdbc":
                return fetchJdbcConfig(metaDataDatasourceId);
            case "kafka":
                return fetchKafkaConfig(metaDataDatasourceId);
            default:
                return Collections.emptyMap();
        }
    }

    @Override
    public Optional<TableSchema> tableSchema(String metaDataTableId) {
        // Fetch table schema from your metadata service
        // Parse the table ID and return the schema
        return Optional.ofNullable(fetchTableSchema(metaDataTableId));
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
     metadata:
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
   - Loads the configured `MetadataProvider` based on `seatunnel.yaml`
   - Calls `init()` with provider-specific configuration

2. **Job Submission**
   - Parses job configuration
   - Detects presence of `metadata_datasource_id` in connector configs

3. **Configuration Fetching**
   - Calls `provider.datasourceMap(connectorIdentifier, metaDataDatasourceId)` to retrieve configuration from external system
   - The provider queries the metadata service and returns connector configuration

4. **Configuration Merge**
   - Merges fetched configuration with job-level parameters
   - Job-level parameters take precedence

5. **Schema Retrieval** (when applicable)
   - Calls `provider.tableSchema(metaDataTableId)` to fetch table schema
   - The provider returns the table schema with column definitions and types
