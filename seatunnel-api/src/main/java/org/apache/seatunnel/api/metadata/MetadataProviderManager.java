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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueType;

import org.apache.seatunnel.api.metadata.exception.MetadataProviderException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.common.config.TypesafeConfigUtils;
import org.apache.seatunnel.common.constants.PluginType;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * x Utility class for resolving data source configurations from MetaData Center.
 *
 * <p>This utility provides methods to merge connection configurations retrieved from external
 * metadata services (via {@link MetadataProvider}) into SeaTunnel connector configurations.
 */
@Slf4j
public final class MetadataProviderManager {

    /** Cache for initialized MetadataProvider instance. */
    private static volatile MetadataProvider cachedProvider = null;

    /**
     * Resolves and merges data source configurations for a SeaTunnel job config.
     *
     * @param seaTunnelJobConfig the SeaTunnel job configuration (Hocon/Config format)
     * @param metaDataConfig the MetaData configuration containing provider kind and properties
     * @return a new Config with datasource configurations merged
     */
    public static Config resolveDataSourceConfigs(
            Config seaTunnelJobConfig, MetadataConfig metaDataConfig) {
        if (!metaDataConfig.isEnabled()) {
            log.debug("MetaData Center is disabled, returning original config");
            return seaTunnelJobConfig;
        }

        String providerKind = metaDataConfig.getKind();
        log.info("Starting datasource config resolution with provider: {}", providerKind);

        // Get or create initialized provider instance (cached with lazy loading)
        MetadataProvider provider =
                getOrCreateProvider(
                        providerKind, ConfigFactory.parseMap(metaDataConfig.getProperties()));

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

    public static Optional<TableSchema> resolveTableSchema(
            String metaDataTableId, MetadataConfig metaDataConfig) {
        if (metaDataConfig == null || !metaDataConfig.isEnabled()) {
            return Optional.empty();
        }
        MetadataProvider provider =
                getOrCreateProvider(
                        metaDataConfig.getKind(),
                        ConfigFactory.parseMap(metaDataConfig.getProperties()));
        return provider.tableSchema(metaDataTableId);
    }

    /**
     * Gets or creates an initialized MetadataProvider instance with lazy loading caching.
     *
     * <p>Thread-safety: Uses double-checked locking - first check outside lock for fast path,
     * re-check inside lock to handle concurrent modifications.
     *
     * @param kind the provider kind (e.g., "gravitino", "datahub")
     * @param config the configuration for the provider
     * @return initialized MetadataProvider instance
     */
    private static MetadataProvider getOrCreateProvider(String kind, Config config) {
        // First check: fast path - return if cached provider matches requested kind
        MetadataProvider provider = cachedProvider;
        if (provider != null && provider.kind().equalsIgnoreCase(kind)) {
            return provider;
        }
        synchronized (MetadataProviderManager.class) {
            // Re-read volatile variable to see other threads' updates
            provider = cachedProvider;
            // Handle kind mismatch: close old provider before creating new one
            if (provider != null && !provider.kind().equalsIgnoreCase(kind)) {
                log.info("Provider kind changed from {} to {}", provider.kind(), kind);
                try {
                    provider.close();
                } catch (Exception e) {
                    log.warn(
                            "Failed to close old MetadataProvider of kind: {}", provider.kind(), e);
                }
                cachedProvider = null;
                provider = null;
            }
            // Create new provider if needed (null or just closed due to kind mismatch)
            if (provider == null) {
                provider = MetadataProviderFactory.getProvider(kind);
                provider.init(config);
                cachedProvider = provider;
                log.info("Created and cached new MetadataProvider: {}", kind);
            }
        }
        return provider;
    }

    /**
     * Resolves and merges data source configuration for a single connector config.
     *
     * <p>If the config contains a {@code metadata_datasource_id}, this method will:
     *
     * <ol>
     *   <li>Use the provided {@link MetadataProvider} (already initialized)
     *   <li>Fetch the connection config from the metadata service using the metadata_datasource_id
     *   <li>Merge the fetched config into the original config
     * </ol>
     *
     * @param connectorConfig the connector configuration
     * @param provider the initialized MetadataProvider instance
     * @param providerKind the kind of MetadataProvider (e.g., "gravitino", "datahub")
     * @return a new Config with datasource configuration merged, or the original config if no
     *     metadata_datasource_id is present
     */
    private static Config resolveConnectorConfig(
            Config connectorConfig, MetadataProvider provider, String providerKind) {
        Optional<String> datasourceIdOptional = getDatasourceId(connectorConfig);

        if (!datasourceIdOptional.isPresent()) {
            log.debug(
                    "No metadata_datasource_id found in connector config at path: {}, returning original config",
                    connectorConfig.origin().description());
            return connectorConfig;
        }

        String datasourceId = datasourceIdOptional.get();
        String connectorIdentifier = getConnectorIdentifier(connectorConfig);

        log.info(
                "Resolving datasource config for connector: {}, metadata_datasource_id: {}, provider: {}",
                connectorIdentifier,
                datasourceId,
                providerKind);

        try {
            // Fetch connection config from metadata service via provider
            Map<String, Object> datasourceConfig =
                    provider.datasourceMap(connectorIdentifier, datasourceId);

            if (datasourceConfig == null || datasourceConfig.isEmpty()) {
                log.warn(
                        "Received empty or null config from MetadataProvider for metadata_datasource_id: {}",
                        datasourceId);
                return connectorConfig;
            }

            // Merge the fetched config into the original config
            return mergeConfig(connectorConfig, datasourceConfig, datasourceId);

        } catch (MetadataProviderException e) {
            throw e;
        } catch (Exception e) {
            throw new MetadataProviderException(
                    String.format(
                            "Failed to resolve datasource config for connector: %s, metadata_datasource_id: %s, provider: %s",
                            connectorIdentifier, datasourceId, providerKind),
                    e);
        }
    }

    /**
     * Gets the metadata_datasource_id from a connector config.
     *
     * <p>This method first checks at the root level, then looks inside the nested connector config.
     *
     * @param config the connector configuration
     * @return Optional containing the metadata_datasource_id if present, empty otherwise
     */
    private static Optional<String> getDatasourceId(Config config) {
        try {
            // First check at root level (for configs like { Jdbc: {...}, metadata_datasource_id:
            // "ds-123" })
            if (config.hasPath(ConnectorCommonOptions.METADATA_DATASOURCE_ID.key())) {
                return Optional.of(
                        config.getString(ConnectorCommonOptions.METADATA_DATASOURCE_ID.key()));
            }

            // If not found at root, check inside the nested connector config
            // (for configs like { Jdbc: { metadata_datasource_id: "ds-123", ... } })
            String connectorIdentifier = getConnectorIdentifier(config);
            if (!"unknown".equals(connectorIdentifier)) {
                Config nestedConfig = config.getConfig(connectorIdentifier);
                if (nestedConfig.hasPath(ConnectorCommonOptions.METADATA_DATASOURCE_ID.key())) {
                    return Optional.of(
                            nestedConfig.getString(
                                    ConnectorCommonOptions.METADATA_DATASOURCE_ID.key()));
                }
            }
        } catch (ConfigException e) {
            log.debug("Failed to get metadata_datasource_id from config", e);
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
     * @param datasourceConfig the configuration fetched from MetaData Center
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
            log.debug(
                    "Merging datasource config: key={}, metadata_datasource_id={}",
                    key,
                    datasourceId);

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
                "Successfully merged datasource config for metadata_datasource_id: {}, connector: {}, merged keys count: {}",
                datasourceId,
                connectorIdentifier,
                datasourceConfig.size());

        return mergedConfig;
    }

    /**
     * Closes the cached MetadataProvider instance.
     *
     * <p>This method should be called when the application shuts down (e.g., when the SeaTunnel
     * Server or Client is stopping) to properly release all resources held by the provider.
     *
     * <p>This method is idempotent and can be safely called multiple times.
     */
    public static void closeProviders() {
        MetadataProvider provider = cachedProvider;
        if (provider != null) {
            try {
                log.info("Closing cached MetadataProvider");
                provider.close();
            } catch (Exception e) {
                log.warn("Failed to close MetadataProvider", e);
            }
            cachedProvider = null;
        }
        log.info("MetadataProvider closed");
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
