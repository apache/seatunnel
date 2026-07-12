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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Data
public class HugeGraphSinkConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(HugeGraphSinkConfig.class);

    // Shared connection config
    private HugeGraphConnectionConfig connectionConfig;

    // Batch config
    private int batchSize;
    private int batchIntervalMs;
    private int maxRetries;
    private int retryBackoffMs;

    // New: multi-mapping config
    private List<MappingConfig> mappings;
    private HugeGraphSchemaSaveMode schemaSaveMode;
    private boolean deleteVertexWithEdges;

    // Legacy (deprecated, kept for backward compat parsing only)
    private SchemaConfig schemaConfig;
    private List<String> selectedFields;
    private List<String> ignoredFields;

    public static HugeGraphSinkConfig of(ReadonlyConfig config) {
        HugeGraphSinkConfig sinkConfig = new HugeGraphSinkConfig();

        // Connection
        sinkConfig.setConnectionConfig(HugeGraphConnectionConfig.of(config));

        // Batch
        sinkConfig.setBatchSize(
                config.getOptional(HugeGraphOptions.BATCH_SIZE)
                        .orElse(HugeGraphOptions.BATCH_SIZE.defaultValue()));
        sinkConfig.setBatchIntervalMs(
                config.getOptional(HugeGraphOptions.BATCH_INTERVAL_MS)
                        .orElse(HugeGraphOptions.BATCH_INTERVAL_MS.defaultValue()));
        sinkConfig.setMaxRetries(sinkConfig.getConnectionConfig().getMaxRetries());
        sinkConfig.setRetryBackoffMs(sinkConfig.getConnectionConfig().getRetryBackoffMs());

        // Resolve mappings with backward compatibility
        sinkConfig.setMappings(resolveMappings(config, sinkConfig));
        applyMappingDefaults(sinkConfig.getMappings());

        boolean legacyConfig = sinkConfig.getSchemaConfig() != null;
        sinkConfig.setSchemaSaveMode(
                config.getOptional(HugeGraphSinkOptions.SCHEMA_SAVE_MODE)
                        .orElse(
                                legacyConfig
                                        ? HugeGraphSchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST
                                        : HugeGraphSinkOptions.SCHEMA_SAVE_MODE.defaultValue()));
        sinkConfig.setDeleteVertexWithEdges(
                config.getOptional(HugeGraphSinkOptions.DELETE_VERTEX_WITH_EDGES)
                        .orElse(
                                legacyConfig
                                        ? true
                                        : HugeGraphSinkOptions.DELETE_VERTEX_WITH_EDGES
                                                .defaultValue()));

        // Deprecated fields (parse but warn)
        config.getOptional(HugeGraphSinkOptions.SELECTED_FIELDS)
                .ifPresent(
                        fields -> {
                            LOG.warn(
                                    "Option 'selected_fields' is deprecated. Use 'properties' within each mapping instead.");
                            sinkConfig.setSelectedFields(fields);
                        });
        config.getOptional(HugeGraphSinkOptions.IGNORED_FIELDS)
                .ifPresent(
                        fields -> {
                            LOG.warn(
                                    "Option 'ignored_fields' is deprecated. Use 'properties' within each mapping instead.");
                            sinkConfig.setIgnoredFields(fields);
                        });

        return sinkConfig;
    }

    /**
     * Converts legacy global field selection into the mapping property list. The old writer ignored
     * {@code schema_config.properties} and wrote all fields after applying selected/ignored fields,
     * so preserving that behavior requires the input row schema.
     */
    public void applyLegacyFieldSelection(SeaTunnelRowType rowType) {
        if (schemaConfig == null || mappings == null || mappings.isEmpty()) {
            return;
        }

        List<String> effectiveFields;
        if (selectedFields != null && !selectedFields.isEmpty()) {
            effectiveFields = new ArrayList<>(selectedFields);
        } else {
            effectiveFields = new ArrayList<>(Arrays.asList(rowType.getFieldNames()));
            if (ignoredFields != null && !ignoredFields.isEmpty()) {
                Set<String> ignored = new HashSet<>(ignoredFields);
                effectiveFields.removeIf(ignored::contains);
            }
        }
        mappings.get(0).setProperties(effectiveFields);
    }

    private static List<MappingConfig> resolveMappings(
            ReadonlyConfig config, HugeGraphSinkConfig sinkConfig) {
        boolean hasMappings = config.getOptional(HugeGraphSinkOptions.MAPPINGS).isPresent();
        boolean hasSchemaConfig =
                config.getOptional(HugeGraphSinkOptions.SCHEMA_CONFIG).isPresent();

        if (hasMappings) {
            if (hasSchemaConfig) {
                LOG.warn(
                        "Both 'mappings' and 'schema_config' are present. "
                                + "'schema_config' will be ignored. Please migrate to 'mappings'.");
            }
            return config.get(HugeGraphSinkOptions.MAPPINGS);
        }

        if (hasSchemaConfig) {
            SchemaConfig schemaConfig = config.get(HugeGraphSinkOptions.SCHEMA_CONFIG);
            sinkConfig.setSchemaConfig(schemaConfig);
            return Collections.singletonList(MappingConfig.fromLegacySchemaConfig(schemaConfig));
        }

        throw new HugeGraphConnectorException(
                HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                "Either 'mappings' or 'schema_config' must be specified. "
                        + "'mappings' is the recommended option.");
    }

    private static void applyMappingDefaults(List<MappingConfig> mappings) {
        if (mappings == null) {
            return;
        }
        for (MappingConfig m : mappings) {
            if (m.getDateFormat() == null || m.getDateFormat().isEmpty()) {
                m.setDateFormat("yyyy-MM-dd");
            }
            if (m.getTimeZone() == null || m.getTimeZone().isEmpty()) {
                m.setTimeZone("GMT+8");
            }
        }
    }
}
