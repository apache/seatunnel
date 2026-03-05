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

import java.util.Map;

/**
 * Mapper for converting external data source metadata to connector configuration.
 *
 * <p>Implementations bridge the gap between external metadata systems and SeaTunnel connectors by
 * transforming metadata into connector-specific configuration maps.
 *
 * <h2>Resource Management </h2>
 *
 * <p>Mappers should NOT hold resources that require cleanup (e.g., HTTP clients, JDBC connections).
 * Instead:
 *
 * <ul>
 *   <li>Receive resources via constructor from the parent {@link DataSourceProvider}
 *   <li>Use resources to perform {@link #map(String)} operations
 *   <li>Let the provider handle resource cleanup via {@link DataSourceProvider#close()}
 * </ul>
 *
 * <h2>Thread Safety </h2>
 *
 * <p>Mapper instances may be called concurrently by multiple threads. Implementations must be
 * thread-safe.
 */
public interface DataSourceMapper {

    /**
     * Returns the connector identifier this mapper supports.
     *
     * <p>The identifier should match the SeaTunnel connector's plugin identifier (e.g., "jdbc",
     * "mysql-cdc", "kafka").
     *
     * @return connector identifier
     */
    String connectorIdentifier();

    /**
     * Maps the given data source ID to connector configuration.
     *
     * <p>This method retrieves metadata from the external system for the specified data source and
     * converts it into a configuration map compatible with the target connector.
     *
     * @param datasourceId the data source ID in the external metadata system
     * @return configuration map for the connector, or null if mapping fails
     */
    Map<String, Object> map(String datasourceId);
}
