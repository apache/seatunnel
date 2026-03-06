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

package org.apache.seatunnel.api.datasource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.util.Collection;

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
 *   <li>{@link #init(ReadonlyConfig)} is called once during startup with configuration from {@code
 *       seatunnel.yaml}
 *   <li>{@link #dataSourceMappers()} is called to obtain mappers for resolving {@code datasourceId}
 *       in job configs
 *   <li>{@link #close()} is called once during client shutdown
 * </ol>
 *
 * <h2>Resource Management </h2>
 *
 * <p>Providers are responsible for managing all resources needed by their mappers:
 *
 * <ul>
 *   <li>HTTP clients for REST API calls
 *   <li>Connection pools for JDBC/Redis access
 *   <li>Any other shared resources
 * </ul>
 *
 * <p>{@link DataSourceMapper} implementations should NOT hold resources directly; they should
 * obtain resources from the provider. This ensures:
 *
 * <ul>
 *   <li>Resources are created once in {@link #init(ReadonlyConfig)}
 *   <li>Resources are reused across multiple mapper calls
 *   <li>Resources are cleaned up in {@link #close()}
 * </ul>
 *
 * <h2>Thread Safety </h2>
 *
 * <p>Provider instances may be accessed concurrently by multiple threads. Implementations must be
 * thread-safe.
 */
public interface DataSourceProvider extends AutoCloseable {

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
     * Returns the collection of data source mappers supported by this provider.
     *
     * <p>Each mapper is responsible for converting metadata from a specific connector type into
     * SeaTunnel configuration.
     *
     * @return collection of supported data source mappers
     */
    Collection<DataSourceMapper> dataSourceMappers();

    /**
     * Gets the data source mapper for a specific connector identifier.
     *
     * <p>This is a convenience method for directly looking up a mapper by connector identifier.
     * Default implementation iterates through the collection, but subclasses may override for
     * better performance (e.g., using a map for O(1) lookup).
     *
     * @param connectorIdentifier the connector identifier (e.g., "Jdbc", "Kafka")
     * @return the matching mapper, or null if not found
     */
    default DataSourceMapper getMapper(String connectorIdentifier) {
        for (DataSourceMapper mapper : dataSourceMappers()) {
            if (mapper.connectorIdentifier().equalsIgnoreCase(connectorIdentifier)) {
                return mapper;
            }
        }
        return null;
    }

    /** Closes resources held by this provider. */
    @Override
    void close();
}
