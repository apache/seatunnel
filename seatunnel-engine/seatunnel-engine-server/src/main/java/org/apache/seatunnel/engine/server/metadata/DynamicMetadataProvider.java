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

package org.apache.seatunnel.engine.server.metadata;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.metadata.MetadataProvider;
import org.apache.seatunnel.api.metadata.exception.MetadataProviderException;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.engine.core.job.DynamicMetadataDataSource;

import com.google.auto.service.AutoService;
import com.hazelcast.map.IMap;
import lombok.extern.slf4j.Slf4j;
import shade.org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Dynamic implementation of {@link MetadataProvider}.
 *
 * <p>This provider stores and retrieves datasource configurations from Hazelcast distributed IMap,
 * allowing users to register datasources through REST API and reference them in job configs.
 *
 * <p>Configuration (from seatunnel.yaml):
 *
 * <pre>
 * seatunnel:
 *   metadata:
 *     enabled: true
 *     kind: dynamic
 * </pre>
 *
 * <p>Usage in job config:
 *
 * <pre>
 * source {
 *   Jdbc {
 *     metadata_datasource_id = "mysql_001"
 *     query = "select * from user"
 *   }
 * }
 * </pre>
 */
@Slf4j
@AutoService(MetadataProvider.class)
public class DynamicMetadataProvider implements MetadataProvider {

    /** Hazelcast instance reference, set by SeaTunnelServer during initialization */
    private static volatile IMap<String, DynamicMetadataDataSource> datasourceIMap;

    /**
     * Set the Hazelcast instance for this provider.
     *
     * <p>This method is called by SeaTunnelServer during initialization.
     *
     * @param iMap the Hazelcast instance
     */
    public static void setMetadataDatasourceImap(IMap<String, DynamicMetadataDataSource> iMap) {
        datasourceIMap = iMap;
        log.info("DynamicMetadataProvider: Hazelcast instance set");
    }

    @Override
    public String kind() {
        return "dynamic";
    }

    @Override
    public void init(Config config) {}

    @Override
    public Map<String, Object> datasourceMap(
            String connectorIdentifier, String metaDataDatasourceId) {
        if (datasourceIMap == null) {
            throw new MetadataProviderException(
                    "DynamicMetadataProvider is not properly initialized. "
                            + "Hazelcast IMap is not available.");
        }

        if (StringUtils.isEmpty(metaDataDatasourceId)) {
            throw new MetadataProviderException("metadata_datasource_id cannot be null or empty");
        }

        log.info(
                "Fetching datasource configuration for connectorType={}, metaDataDatasourceId={}",
                connectorIdentifier,
                metaDataDatasourceId);

        // Get datasource from IMap
        DynamicMetadataDataSource dataSource = datasourceIMap.get(metaDataDatasourceId);
        if (dataSource == null) {
            throw new MetadataProviderException(
                    String.format(
                            "Datasource with id '%s' not found. "
                                    + "Please register it through REST API first.",
                            metaDataDatasourceId));
        }

        // Validate connector type matches (case-insensitive)
        String storedConnectorType = dataSource.getConnectorType();
        if (!connectorIdentifier.equalsIgnoreCase(storedConnectorType)) {
            throw new MetadataProviderException(
                    String.format(
                            "Connector type mismatch. Expected '%s' but datasource '%s' has type '%s'",
                            connectorIdentifier, metaDataDatasourceId, storedConnectorType));
        }

        // Return properties as Map<String, Object>
        Map<String, Object> properties = dataSource.getProperties();
        if (properties == null || properties.isEmpty()) {
            log.warn("Datasource '{}' has no properties defined", metaDataDatasourceId);
            return Collections.emptyMap();
        }

        log.info("Successfully retrieved datasource configuration for '{}'", metaDataDatasourceId);

        // Convert to Map<String, Object> for compatibility
        return Collections.unmodifiableMap(properties);
    }

    @Override
    public Optional<TableSchema> tableSchema(String metaDataTableId) {
        throw new UnsupportedOperationException(
                "Table schema retrieval is not supported by DynamicMetadataProvider. "
                        + "Dynamic metadata provider only supports datasource configuration management through REST API, "
                        + "not table schema discovery.");
    }

    @Override
    public void close() {
        datasourceIMap = null;
    }
}
