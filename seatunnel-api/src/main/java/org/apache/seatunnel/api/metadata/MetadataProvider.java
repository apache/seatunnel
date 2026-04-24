/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.api.metadata;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.table.catalog.TableSchema;

import java.util.Map;
import java.util.Optional;

/**
 * SPI interface for external data source metadata providers.
 *
 * <p>Implementations of this interface are discovered via Java SPI and provide integration with
 * external metadata services (e.g., Gravitino, DataHub, Atlas).
 *
 * <p>The provider acts as an entry point for discovering and mapping data sources to SeaTunnel
 * connectors.
 *
 * <h2>Lifecycle </h2>
 *
 * <ol>
 *   <li>Provider instances are discovered via {@code @AutoService} and cached for the lifetime of
 *       the SeaTunnel client process
 *   <li>{@link #init(Config)} is called once during startup with configuration from {@code
 *       seatunnel.yaml}
 *   <li>{@link #datasourceMap(String, String)} is called to resolve {@code datasourceId} in job
 *       configs
 *   <li>{@link #close()} is called once during client shutdown
 * </ol>
 *
 * <h2>Resource Management </h2>
 *
 * <p>Providers are responsible for managing all resources needed for datasource mapping:
 *
 * <ul>
 *   <li>HTTP clients for REST API calls
 *   <li>Connection pools for JDBC/Redis access
 *   <li>Any other shared resources
 * </ul>
 *
 * <p>This ensures:
 *
 * <ul>
 *   <li>Resources are created once in {@link #init(Config)}
 *   <li>Resources are reused across multiple {@link #datasourceMap(String, String)} calls
 *   <li>Resources are cleaned up in {@link #close()}
 * </ul>
 *
 * <h2>Thread Safety </h2>
 *
 * <p>Provider instances may be accessed concurrently by multiple threads. Implementations must be
 * thread-safe.
 */
public interface MetadataProvider extends AutoCloseable {

    /**
     * Returns a unique identifier for this data source provider.
     *
     * <p>The identifier should match the kind specified in the configuration file (e.g.,
     * "gravitino", "datahub", "atlas"). Use lower case for consistency.
     *
     * @return unique provider identifier
     */
    String kind();

    /**
     * Initializes the provider with the given configuration.
     *
     * @param config the configuration for this provider
     */
    void init(Config config);

    /**
     * Maps the given data source ID to connector configuration.
     *
     * <p>This method retrieves metadata from the external system for the specified data source and
     * converts it into a configuration map compatible with the target connector.
     *
     * @param connectorIdentifier the connector identifier (e.g., "Jdbc", "MySQL-CDC", "Kafka")
     * @param metaDataDatasourceId the data source ID in the external metadata system
     * @return configuration map for the connector, or null if mapping fails
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

    /** Closes resources held by this provider. */
    @Override
    void close();
}
