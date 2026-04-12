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

package org.apache.seatunnel.engine.common.utils;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueType;

import org.apache.seatunnel.api.datasource.DataSourceProvider;
import org.apache.seatunnel.api.datasource.DataSourceProviderFactory;
import org.apache.seatunnel.api.datasource.exception.DataSourceProviderException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.engine.common.config.server.DataSourceConfig;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class for resolving data source configurations from DataSource Center.
 *
 * <p>This utility provides methods to merge connection configurations retrieved from external
 * metadata services (via {@link DataSourceProvider}) into SeaTunnel connector configurations.
 */
@Slf4j
public final class DataSourceConfigResolver {

    /** Cache for initialized DataSourceProvider instance. */
    private static volatile DataSourceProvider cachedProvider = null;

    /**
     * Resolves and merges data source configurations for a SeaTunnel job config.
     *
     * @param seaTunnelJobConfig the SeaTunnel job configuration (Hocon/Config format)
     * @param dataSourceConfig the DataSource configuration containing provider kind and properties
     * @return a new Config with datasource configurations merged
     */
    public static Config resolveDataSourceConfigs(
            Config seaTunnelJobConfig, DataSourceConfig dataSourceConfig) {
        if (!dataSourceConfig.isEnabled()) {
            log.debug("DataSource Center is disabled, returning original config");
            return seaTunnelJobConfig;
        }

        String providerKind = dataSourceConfig.getKind();
        log.info("Starting datasource config resolution with provider: {}", providerKind);

        // Get or create initialized provider instance (cached with lazy loading)
        DataSourceProvider provider =
                getOrCreateProvider(
                        providerKind, ConfigFactory.parseMap(dataSourceConfig.getProperties()));

        // Get original config as unwrapped map
        Map<String, Object> originalMap = seaTunnelJobConfig.root().unwrapped();
        Map<String, Object> resultMap = new HashMap<>(originalMap);

        // Resolve source configs
        List<? extends Config> sourceConfigs =
                TypesafeConfigUtils.getConfigList(
                        seaTunnelJobConfig, PluginType.SOURCE.getType(), Collections.emptyList());
        List<Object> resolvedSources = new ArrayList<>();
        for (Config sourceConfig : sourceConfigs) {
            Config resolved = resolveConnectorConfig(sourceConfig, provider, providerKind);
            resolvedSources.add(resolved.root().unwrapped());
        }
        if (!resolvedSources.isEmpty()) {
            resultMap.put(PluginType.SOURCE.getType(), resolvedSources);
        }

        // Resolve sink configs
        List<? extends Config> sinkConfigs =
                TypesafeConfigUtils.getConfigList(
                        seaTunnelJobConfig, PluginType.SINK.getType(), Collections.emptyList());
        List<Object> resolvedSinks = new ArrayList<>();
        for (Config sinkConfig : sinkConfigs) {
            Config resolved = resolveConnectorConfig(sinkConfig, provider, providerKind);
            resolvedSinks.add(resolved.root().unwrapped());
        }
        if (!resolvedSinks.isEmpty()) {
            resultMap.put(PluginType.SINK.getType(), resolvedSinks);
        }

        return ConfigFactory.parseMap(resultMap);
    }

    /**
     * Gets or creates an initialized DataSourceProvider instance with lazy loading caching.
     *
     * @param kind the provider kind (e.g., "gravitino", "datahub")
     * @param config the configuration for the provider
     * @return initialized DataSourceProvider instance
     */
    private static DataSourceProvider getOrCreateProvider(String kind, Config config) {
        DataSourceProvider provider = cachedProvider;
        if (provider == null) {
            synchronized (DataSourceConfigResolver.class) {
                provider = cachedProvider;
                if (provider == null) {
                    provider = DataSourceProviderFactory.getProvider(kind);
                    provider.init(config);
                    cachedProvider = provider;
                    log.info("Created and cached new DataSourceProvider: {}", kind);
                }
            }
        }
        return provider;
    }

    /**
     * Resolves and merges data source configuration for a single connector config.
     *
     * <p>If the config contains a {@code datasource_id}, this method will:
     *
     * <ol>
     *   <li>Use the provided {@link DataSourceProvider} (already initialized)
     *   <li>Fetch the connection config from the metadata service using the datasource_id
     *   <li>Merge the fetched config into the original config
     * </ol>
     *
     * @param connectorConfig the connector configuration
     * @param provider the initialized DataSourceProvider instance
     * @param providerKind the kind of DataSourceProvider (e.g., "gravitino", "datahub")
     * @return a new Config with datasource configuration merged, or the original config if no
     *     datasource_id is present
     */
    private static Config resolveConnectorConfig(
            Config connectorConfig, DataSourceProvider provider, String providerKind) {
        Optional<String> datasourceIdOptional = getDatasourceId(connectorConfig);

        if (!datasourceIdOptional.isPresent()) {
            log.debug(
                    "No datasource_id found in connector config at path: {}, returning original config",
                    connectorConfig.origin().description());
            return connectorConfig;
        }

        String datasourceId = datasourceIdOptional.get();
        String connectorIdentifier = getConnectorIdentifier(connectorConfig);

        log.info(
                "Resolving datasource config for connector: {}, datasource_id: {}, provider: {}",
                connectorIdentifier,
                datasourceId,
                providerKind);

        try {
            // Fetch connection config from metadata service via provider
            Map<String, Object> datasourceConfig =
                    provider.datasourceMap(connectorIdentifier, datasourceId);

            if (datasourceConfig == null || datasourceConfig.isEmpty()) {
                log.warn(
                        "Received empty or null config from DataSourceProvider for datasource_id: {}",
                        datasourceId);
                return connectorConfig;
            }

            // Merge the fetched config into the original config
            return mergeConfig(connectorConfig, datasourceConfig, datasourceId);

        } catch (DataSourceProviderException e) {
            throw e;
        } catch (Exception e) {
            throw new DataSourceProviderException(
                    String.format(
                            "Failed to resolve datasource config for connector: %s, datasource_id: %s, provider: %s",
                            connectorIdentifier, datasourceId, providerKind),
                    e);
        }
    }

    /**
     * Gets the datasource_id from a connector config.
     *
     * <p>This method first checks at the root level, then looks inside the nested connector config.
     *
     * @param config the connector configuration
     * @return Optional containing the datasource_id if present, empty otherwise
     */
    private static Optional<String> getDatasourceId(Config config) {
        try {
            // First check at root level (for configs like { Jdbc: {...}, datasource_id: "ds-123" })
            if (config.hasPath(ConnectorCommonOptions.DATASOURCE_ID.key())) {
                return Optional.of(config.getString(ConnectorCommonOptions.DATASOURCE_ID.key()));
            }

            // If not found at root, check inside the nested connector config
            // (for configs like { Jdbc: { datasource_id: "ds-123", ... } })
            String connectorIdentifier = getConnectorIdentifier(config);
            if (!"unknown".equals(connectorIdentifier)) {
                Config nestedConfig = config.getConfig(connectorIdentifier);
                if (nestedConfig.hasPath(ConnectorCommonOptions.DATASOURCE_ID.key())) {
                    return Optional.of(
                            nestedConfig.getString(ConnectorCommonOptions.DATASOURCE_ID.key()));
                }
            }
        } catch (ConfigException e) {
            log.debug("Failed to get datasource_id from config", e);
        }
        return Optional.empty();
    }

    /**
     * Gets the connector identifier (plugin name) from a connector config.
     *
     * <p>This method first checks for plugin_name in the config (from TypesafeConfigUtils
     * processing), then looks for a nested object structure.
     *
     * @param config the connector configuration
     * @return the connector identifier or "unknown" if not found
     */
    private static String getConnectorIdentifier(Config config) {
        // First check if plugin_name is present (added by TypesafeConfigUtils)
        try {
            if (config.hasPath(ConnectorCommonOptions.PLUGIN_NAME.key())) {
                return config.getString(ConnectorCommonOptions.PLUGIN_NAME.key());
            }
        } catch (ConfigException e) {
            // Ignore, try the nested structure approach
        }

        // Fallback: look for nested object structure (original config format)
        for (Map.Entry<String, ConfigValue> entry : config.root().entrySet()) {
            if (entry.getValue().valueType() == ConfigValueType.OBJECT) {
                return entry.getKey();
            }
        }
        return "unknown";
    }

    /**
     * Merges the datasource configuration into the original connector config.
     *
     * <p>The datasource config values will override values in the original config if keys conflict.
     *
     * @param connectorConfig the original connector configuration
     * @param datasourceConfig the configuration fetched from DataSource Center
     * @param datasourceId the datasource ID (for logging purposes)
     * @return a new Config with merged configurations
     */
    private static Config mergeConfig(
            Config connectorConfig, Map<String, Object> datasourceConfig, String datasourceId) {
        // Get the connector identifier (plugin name)
        String connectorIdentifier = getConnectorIdentifier(connectorConfig);

        // Check if this is the flat structure from TypesafeConfigUtils (has plugin_name field)
        boolean isFlatStructure = connectorConfig.hasPath(ConnectorCommonOptions.PLUGIN_NAME.key());

        Map<String, Object> originalMap;
        if (isFlatStructure) {
            // Flat structure: directly use the config's root map
            originalMap = new HashMap<>(connectorConfig.root().unwrapped());
        } else {
            // Nested structure: get the nested config inside the plugin
            Config originalNestedConfig = connectorConfig.getConfig(connectorIdentifier);
            originalMap = new HashMap<>(originalNestedConfig.root().unwrapped());
            // Also include the plugin_name as the root key
            Map<String, Object> wrapperMap = new HashMap<>();
            wrapperMap.put(connectorIdentifier, originalMap);
            originalMap = wrapperMap;
        }

        // Create merged map
        Map<String, Object> mergedMap = new HashMap<>(originalMap);

        // Merge datasource config - values from datasourceConfig will override
        for (Map.Entry<String, Object> entry : datasourceConfig.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            log.debug("Merging datasource config: key={}, datasource_id={}", key, datasourceId);

            if (isFlatStructure) {
                // Flat structure: merge directly into the map
                mergedMap.put(key, value);
            } else {
                // Nested structure: merge into the nested map
                @SuppressWarnings("unchecked")
                Map<String, Object> nestedMap =
                        (Map<String, Object>) mergedMap.get(connectorIdentifier);
                if (nestedMap != null) {
                    nestedMap.put(key, value);
                }
            }
        }

        Config mergedConfig = ConfigFactory.parseMap(mergedMap);

        log.info(
                "Successfully merged datasource config for datasource_id: {}, connector: {}, merged keys count: {}",
                datasourceId,
                connectorIdentifier,
                datasourceConfig.size());

        return mergedConfig;
    }

    /**
     * Closes the cached DataSourceProvider instance.
     *
     * <p>This method should be called when the application shuts down (e.g., when the SeaTunnel
     * Server or Client is stopping) to properly release all resources held by the provider.
     *
     * <p>This method is idempotent and can be safely called multiple times.
     */
    public static void closeProviders() {
        DataSourceProvider provider = cachedProvider;
        if (provider != null) {
            try {
                log.info("Closing cached DataSourceProvider");
                provider.close();
            } catch (Exception e) {
                log.warn("Failed to close DataSourceProvider", e);
            }
            cachedProvider = null;
        }
        log.info("DataSourceProvider closed");
    }

    /**
     * Clears the provider cache.
     *
     * <p>This method is primarily intended for testing purposes. It closes the cached provider and
     * clears the cache.
     */
    @VisibleForTesting
    public static void clearCache() {
        closeProviders();
    }
}
