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
    private boolean batchFailureFallback;
    private boolean checkVertex;
    private int maxRetries;
    private int retryBackoffMs;
    // Max records the single-record fallback may skip before the task fails (-1 = unlimited).
    private int maxInsertErrors;
    // Optional directory to persist skipped-record failure samples; null = do not persist.
    private String failureDataPath;

    // New: multi-mapping config
    private List<MappingConfig> mappings;
    private HugeGraphSchemaSaveMode schemaSaveMode;
    private HugeGraphDataSaveMode dataSaveMode;
    private boolean deleteVertexWithEdges;
    private boolean allowCascadeDeleteUnmappedEdges;

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
        sinkConfig.setBatchFailureFallback(
                config.getOptional(HugeGraphOptions.BATCH_FAILURE_FALLBACK)
                        .orElse(HugeGraphOptions.BATCH_FAILURE_FALLBACK.defaultValue()));
        sinkConfig.setCheckVertex(
                config.getOptional(HugeGraphOptions.CHECK_VERTEX)
                        .orElse(HugeGraphOptions.CHECK_VERTEX.defaultValue()));
        sinkConfig.setMaxRetries(sinkConfig.getConnectionConfig().getMaxRetries());
        sinkConfig.setRetryBackoffMs(sinkConfig.getConnectionConfig().getRetryBackoffMs());
        sinkConfig.setMaxInsertErrors(
                config.getOptional(HugeGraphOptions.MAX_INSERT_ERRORS)
                        .orElse(HugeGraphOptions.MAX_INSERT_ERRORS.defaultValue()));
        config.getOptional(HugeGraphOptions.FAILURE_DATA_PATH)
                .ifPresent(sinkConfig::setFailureDataPath);

        // Resolve mappings with backward compatibility
        sinkConfig.setMappings(resolveMappings(config, sinkConfig));
        applyMappingDefaults(sinkConfig.getMappings());

        // Multi-table contract: source_table is an ALL-or-NOTHING switch.
        // All absent → single-table backward-compatible (each mapping activates in the one writer).
        // All present → multi-table (each mapping activates only in its matching writer).
        // Mixed → misconfiguration; fail fast with a clear diagnostic.
        if (sinkConfig.getMappings() != null) {
            validateSourceTableConsistency(sinkConfig.getMappings());
        }

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
        sinkConfig.setAllowCascadeDeleteUnmappedEdges(
                config.getOptional(HugeGraphSinkOptions.ALLOW_CASCADE_DELETE_UNMAPPED_EDGES)
                        .orElse(
                                HugeGraphSinkOptions.ALLOW_CASCADE_DELETE_UNMAPPED_EDGES
                                        .defaultValue()));
        sinkConfig.setDataSaveMode(
                config.getOptional(HugeGraphSinkOptions.DATA_SAVE_MODE)
                        .orElse(HugeGraphSinkOptions.DATA_SAVE_MODE.defaultValue()));

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
            // Leave timeZone unset when the user did not configure one; DataTypeUtil then falls
            // back to ZoneId.systemDefault(), matching the HugeGraph Source. Hard-coding GMT+8
            // here previously silently shifted absolute times by up to 8 hours when the Source
            // ran on a JVM whose default zone was not Asia/Shanghai.
        }
    }

    /**
     * Enforces the ALL-or-NOTHING contract on {@code source_table}.
     *
     * <p>All mappings with {@code source_table} set → multi-table mode: each mapping activates only
     * in the writer whose {@code CatalogTable.getTablePath()} matches. All mappings without {@code
     * source_table} → single-table backward-compatible: every mapping activates in the one writer.
     * A mix of set and unset is ambiguous — the user either forgot to add {@code source_table} to
     * some mappings, or accidentally added it to one. Refuse with a clear diagnostic.
     */
    static void validateSourceTableConsistency(List<MappingConfig> mappings) {
        boolean anySet = false;
        boolean anyUnset = false;
        List<String> setLabels = new ArrayList<>();
        List<String> unsetLabels = new ArrayList<>();

        for (MappingConfig m : mappings) {
            if (m.getSourceTable() == null || m.getSourceTable().isEmpty()) {
                anyUnset = true;
                unsetLabels.add(m.getLabel());
            } else {
                anySet = true;
                setLabels.add(m.getLabel());
            }
        }

        if (anySet && anyUnset) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                    String.format(
                            "Inconsistent 'source_table' configuration. %d mapping(s) set it (%s), "
                                    + "but %d mapping(s) are missing it (%s). "
                                    + "'source_table' is an ALL-or-NOTHING switch: either every "
                                    + "mapping declares it (multi-table mode — each mapping activates "
                                    + "only in the matching writer), or none do (single-table mode — "
                                    + "the backward-compatible default). Check that you haven't "
                                    + "forgotten 'source_table' on some mappings, or added it to one "
                                    + "mapping by mistake.",
                            setLabels.size(), setLabels, unsetLabels.size(), unsetLabels));
        }
    }
}
