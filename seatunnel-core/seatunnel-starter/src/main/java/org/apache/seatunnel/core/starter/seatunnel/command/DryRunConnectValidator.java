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

package org.apache.seatunnel.core.starter.seatunnel.command;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.table.CatalogOptions;
import org.apache.seatunnel.api.options.table.ColumnOptions;
import org.apache.seatunnel.api.options.table.FieldOptions;
import org.apache.seatunnel.api.options.table.TableSchemaOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.SupportSinkDryRunValidation;
import org.apache.seatunnel.api.table.factory.SupportSourceDryRunValidation;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.DryRunConnectFailureMessageSanitizer;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;
import org.apache.seatunnel.engine.core.parse.ConfigParserUtil;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_INPUT;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_OUTPUT;
import static org.apache.seatunnel.api.table.factory.FactoryUtil.DEFAULT_ID;

/**
 * Performs Layer 1 ({@code --dry-run connect}) validation without creating source/sink runtime
 * objects, readers, writers, committers, or save-mode handlers.
 *
 * <p>Connectivity and schema inference are connector opt-in: only factories implementing {@link
 * SupportSourceDryRunValidation} / {@link SupportSinkDryRunValidation} are actually validated
 * against their external systems. Every other plugin is explicitly reported as {@code SKIPPED} so
 * users are never misled into believing credentials or reachability were checked when they were
 * not.
 *
 * <p>Schema propagation is only trusted when it comes from a validating source factory or from
 * explicit schema metadata in the config ({@code schema} / {@code tableConfigs} / {@code
 * table_list}). When neither is available the framework does NOT fall back to a synthetic
 * placeholder schema; downstream schema checks for that pipeline are reported as {@code SKIPPED}
 * instead.
 */
@Slf4j
class DryRunConnectValidator {

    private final List<? extends Config> sourceConfigs;
    private final List<? extends Config> transformConfigs;
    private final List<? extends Config> sinkConfigs;
    private final ClassLoader sourceAndTransformClassLoader;
    private final ClassLoader sinkClassLoader;

    DryRunConnectValidator(
            List<? extends Config> sourceConfigs,
            List<? extends Config> transformConfigs,
            List<? extends Config> sinkConfigs,
            ClassLoader sourceAndTransformClassLoader,
            ClassLoader sinkClassLoader) {
        this.sourceConfigs = sourceConfigs;
        this.transformConfigs = transformConfigs;
        this.sinkConfigs = sinkConfigs;
        this.sourceAndTransformClassLoader = sourceAndTransformClassLoader;
        this.sinkClassLoader = sinkClassLoader;
    }

    /**
     * Walks the source, transform, and sink DAG, validating each plugin, and returns per-plugin
     * results. Throws {@link ConfigCheckException} on the first validation failure.
     *
     * @return one result per plugin, in DAG evaluation order
     */
    List<PluginResult> validate() {
        ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
        LinkedHashMap<String, SchemaInfo> tableWithSchemas = new LinkedHashMap<>();
        List<PluginResult> results = new ArrayList<>();

        try {
            Thread.currentThread().setContextClassLoader(sourceAndTransformClassLoader);
            for (int configIndex = 0; configIndex < sourceConfigs.size(); configIndex++) {
                Config sourceConfig = sourceConfigs.get(configIndex);
                validateSource(
                        configIndex,
                        sourceConfig,
                        sourceAndTransformClassLoader,
                        tableWithSchemas,
                        results);
            }

            validateTransforms(
                    transformConfigs, sourceAndTransformClassLoader, tableWithSchemas, results);

            Thread.currentThread().setContextClassLoader(sinkClassLoader);
            for (int configIndex = 0; configIndex < sinkConfigs.size(); configIndex++) {
                validateSink(
                        configIndex,
                        sinkConfigs.get(configIndex),
                        sinkClassLoader,
                        tableWithSchemas,
                        results);
            }
        } finally {
            Thread.currentThread().setContextClassLoader(parentClassLoader);
        }

        logSummary(results);
        return results;
    }

    private void validateSource(
            int configIndex,
            Config sourceConfig,
            ClassLoader classLoader,
            LinkedHashMap<String, SchemaInfo> tableWithSchemas,
            List<PluginResult> results) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(sourceConfig);
        String factoryId = ConfigParserUtil.getFactoryId(readonlyConfig);
        String outputId = readonlyConfig.getOptional(PLUGIN_OUTPUT).orElse(DEFAULT_ID);
        try {
            TableSourceFactory factory =
                    FactoryUtil.discoverFactory(classLoader, TableSourceFactory.class, factoryId);
            TableSourceFactoryContext context =
                    new TableSourceFactoryContext(readonlyConfig, classLoader);

            if (factory instanceof SupportSourceDryRunValidation) {
                SupportSourceDryRunValidation validation = (SupportSourceDryRunValidation) factory;
                List<CatalogTable> catalogTables = validation.inferSchemaForDryRun(context);
                if (catalogTables == null || catalogTables.isEmpty()) {
                    throw new ConfigCheckException(
                            location(PluginType.SOURCE, configIndex, factoryId)
                                    + " did not infer any source schema.");
                }
                validation.validateConnectionForDryRun(context, catalogTables);
                tableWithSchemas.put(outputId, SchemaInfo.trusted(catalogTables));
                results.add(
                        PluginResult.validated(
                                PluginType.SOURCE,
                                configIndex,
                                factoryId,
                                "schema inferred and connection validated"));
                return;
            }

            if (hasExplicitSchema(readonlyConfig)) {
                // Schema comes from the user's config, so downstream schema checks stay
                // meaningful even though the connector itself was not contacted.
                List<CatalogTable> catalogTables = factory.discoverTableSchemas(context);
                tableWithSchemas.put(outputId, SchemaInfo.trusted(catalogTables));
                results.add(
                        PluginResult.skipped(
                                PluginType.SOURCE,
                                configIndex,
                                factoryId,
                                "connector does not support connect dry-run validation; "
                                        + "schema taken from config, connectivity NOT verified"));
                return;
            }

            tableWithSchemas.put(outputId, SchemaInfo.unknown());
            results.add(
                    PluginResult.skipped(
                            PluginType.SOURCE,
                            configIndex,
                            factoryId,
                            "connector does not support connect dry-run validation and config "
                                    + "declares no schema fields; connectivity and downstream "
                                    + "schema checks NOT verified"));
        } catch (Exception e) {
            throw wrap(PluginType.SOURCE, configIndex, factoryId, e);
        }
    }

    private void validateTransforms(
            List<? extends Config> configs,
            ClassLoader classLoader,
            LinkedHashMap<String, SchemaInfo> tableWithSchemas,
            List<PluginResult> results) {
        if (configs.isEmpty()) {
            return;
        }
        List<ScheduledTransform> scheduledTransforms =
                scheduleTransforms(configs, tableWithSchemas.keySet());
        int evaluationIndex = 0;
        for (ScheduledTransform scheduledTransform : scheduledTransforms) {
            results.add(
                    validateTransform(
                            evaluationIndex++,
                            scheduledTransform.config,
                            scheduledTransform.legacyFallback,
                            classLoader,
                            tableWithSchemas));
        }
    }

    static List<ScheduledTransform> scheduleTransforms(
            List<? extends Config> configs, Set<String> initialOutputIds) {
        // Index missing inputs once, then release dependents in the legacy queue's evaluation
        // order as each transform output becomes available.
        List<ScheduledTransform> transforms = new ArrayList<>(configs.size());
        Map<String, List<ScheduledTransform>> waitingByInputId = new LinkedHashMap<>();
        NavigableSet<Integer> readyTransformIndexes = new TreeSet<>();
        NavigableSet<Integer> remainingTransformIndexes = new TreeSet<>();
        Set<String> availableOutputIds = new LinkedHashSet<>(initialOutputIds);

        for (int index = 0; index < configs.size(); index++) {
            ScheduledTransform transform = new ScheduledTransform(index, configs.get(index));
            transforms.add(transform);
            Set<String> missingInputIds = new LinkedHashSet<>(transform.inputIds);
            missingInputIds.removeAll(availableOutputIds);
            transform.unresolvedInputCount = missingInputIds.size();
            // Explicit empty input lists are fallback-only and are considered after every other
            // transform resolves.
            if (!transform.inputIds.isEmpty()) {
                if (missingInputIds.isEmpty()) {
                    readyTransformIndexes.add(index);
                } else {
                    for (String missingInputId : missingInputIds) {
                        waitingByInputId
                                .computeIfAbsent(missingInputId, ignored -> new ArrayList<>())
                                .add(transform);
                    }
                }
            }
            remainingTransformIndexes.add(index);
        }

        List<ScheduledTransform> orderedTransforms = new ArrayList<>(transforms.size());
        int queueHeadIndex = 0;
        while (!readyTransformIndexes.isEmpty()) {
            Integer transformIndex = readyTransformIndexes.ceiling(queueHeadIndex);
            if (transformIndex == null) {
                transformIndex = readyTransformIndexes.first();
            }
            ScheduledTransform transform = transforms.get(transformIndex);
            transform.scheduled = true;
            orderedTransforms.add(transform);
            readyTransformIndexes.remove(transformIndex);
            remainingTransformIndexes.remove(transformIndex);
            if (!remainingTransformIndexes.isEmpty()) {
                Integer nextQueueHead = remainingTransformIndexes.ceiling(transformIndex);
                queueHeadIndex =
                        nextQueueHead == null ? remainingTransformIndexes.first() : nextQueueHead;
            }
            availableOutputIds.add(transform.outputId);
            for (ScheduledTransform dependent :
                    waitingByInputId.getOrDefault(transform.outputId, Collections.emptyList())) {
                dependent.unresolvedInputCount--;
                if (dependent.unresolvedInputCount == 0) {
                    readyTransformIndexes.add(dependent.configIndex);
                }
            }
        }

        List<ScheduledTransform> unresolvedTransforms =
                transforms.stream()
                        .filter(transform -> !transform.scheduled)
                        .collect(Collectors.toList());
        if (unresolvedTransforms.isEmpty()) {
            return orderedTransforms;
        }

        if (unresolvedTransforms.size() == 1) {
            ScheduledTransform transform = unresolvedTransforms.get(0);
            boolean anyInputAvailable =
                    transform.inputIds.stream().anyMatch(availableOutputIds::contains);
            boolean emptyInputFallback = transform.inputIds.isEmpty();
            boolean singleTransformLegacyFallback = transforms.size() == 1 && !anyInputAvailable;
            if (emptyInputFallback || singleTransformLegacyFallback) {
                transform.legacyFallback = true;
                orderedTransforms.add(transform);
                return orderedTransforms;
            }
        }
        throw unresolvedTransformDependencies(unresolvedTransforms, availableOutputIds);
    }

    private static ConfigCheckException unresolvedTransformDependencies(
            List<ScheduledTransform> transforms, Set<String> availableOutputIds) {
        String unresolvedTransforms =
                transforms.stream()
                        .map(transform -> transform.outputId + " <- " + transform.inputIds)
                        .collect(Collectors.joining(", "));
        return new ConfigCheckException(
                "Unable to resolve transform dependencies: ["
                        + unresolvedTransforms
                        + "]. Available output IDs: "
                        + availableOutputIds
                        + ". Check 'plugin_input' and 'plugin_output' options.");
    }

    private PluginResult validateTransform(
            int configIndex,
            Config transformConfig,
            boolean legacyFallback,
            ClassLoader classLoader,
            LinkedHashMap<String, SchemaInfo> tableWithSchemas) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(transformConfig);
        String factoryId = ConfigParserUtil.getFactoryId(readonlyConfig);
        String outputId = readonlyConfig.getOptional(PLUGIN_OUTPUT).orElse(DEFAULT_ID);
        try {
            List<String> inputIds = getInputIds(readonlyConfig);
            List<SchemaInfo> inputSchemas;
            if (legacyFallback) {
                inputSchemas = Collections.singletonList(findLast(tableWithSchemas));
            } else {
                List<String> missingInputIds =
                        inputIds.stream()
                                .filter(inputId -> !tableWithSchemas.containsKey(inputId))
                                .collect(Collectors.toList());
                if (!missingInputIds.isEmpty()) {
                    throw new ConfigCheckException(
                            "Transform '"
                                    + outputId
                                    + "' is missing scheduled inputs "
                                    + missingInputIds);
                }
                inputSchemas =
                        inputIds.stream().map(tableWithSchemas::get).collect(Collectors.toList());
            }

            if (inputSchemas.stream().anyMatch(SchemaInfo::isUnknown)) {
                tableWithSchemas.put(outputId, SchemaInfo.unknown());
                return PluginResult.skipped(
                        PluginType.TRANSFORM,
                        configIndex,
                        factoryId,
                        "upstream schema not available; schema wiring NOT verified");
            }

            List<CatalogTable> inputCatalogTables =
                    inputSchemas.stream()
                            .map(SchemaInfo::getCatalogTables)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());
            checkCatalogTableTypesEqual(
                    inputCatalogTables, PluginType.TRANSFORM, configIndex, factoryId);
            SeaTunnelTransform<?> transform =
                    FactoryUtil.createAndPrepareMultiTableTransform(
                            new ArrayList<>(new LinkedHashSet<>(inputCatalogTables)),
                            readonlyConfig,
                            classLoader,
                            factoryId);
            tableWithSchemas.put(
                    outputId, SchemaInfo.trusted(transform.getProducedCatalogTables()));
            return PluginResult.validated(
                    PluginType.TRANSFORM, configIndex, factoryId, "schema wiring validated");
        } catch (Exception e) {
            throw wrap(PluginType.TRANSFORM, configIndex, factoryId, e);
        }
    }

    private static String getTransformOutputId(Config transformConfig) {
        return ReadonlyConfig.fromConfig(transformConfig)
                .getOptional(PLUGIN_OUTPUT)
                .orElse(DEFAULT_ID);
    }

    private static List<String> getTransformInputIds(Config transformConfig) {
        return ReadonlyConfig.fromConfig(transformConfig)
                .getOptional(PLUGIN_INPUT)
                .orElse(Collections.singletonList(DEFAULT_ID));
    }

    static final class ScheduledTransform {
        private final int configIndex;
        private final Config config;
        private final String outputId;
        private final List<String> inputIds;
        private int unresolvedInputCount;
        private boolean scheduled;
        private boolean legacyFallback;

        private ScheduledTransform(int configIndex, Config config) {
            this.configIndex = configIndex;
            this.config = config;
            this.outputId = getTransformOutputId(config);
            this.inputIds = getTransformInputIds(config);
        }

        String getOutputId() {
            return outputId;
        }
    }

    private void validateSink(
            int configIndex,
            Config sinkConfig,
            ClassLoader classLoader,
            LinkedHashMap<String, SchemaInfo> tableWithSchemas,
            List<PluginResult> results) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(sinkConfig);
        String factoryId = ConfigParserUtil.getFactoryId(readonlyConfig);
        try {
            TableSinkFactory<?, ?, ?, ?> factory =
                    FactoryUtil.discoverFactory(classLoader, TableSinkFactory.class, factoryId);
            List<SchemaInfo> inputVertices =
                    getInputIds(readonlyConfig).stream()
                            .map(tableWithSchemas::get)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
            if (inputVertices.isEmpty()) {
                inputVertices = Collections.singletonList(findLast(tableWithSchemas));
            }

            if (inputVertices.stream().anyMatch(SchemaInfo::isUnknown)) {
                results.add(
                        PluginResult.skipped(
                                PluginType.SINK,
                                configIndex,
                                factoryId,
                                "upstream schema not available; connectivity and schema "
                                        + "compatibility NOT verified"));
                return;
            }

            if (!(factory instanceof SupportSinkDryRunValidation)) {
                results.add(
                        PluginResult.skipped(
                                PluginType.SINK,
                                configIndex,
                                factoryId,
                                "connector does not support connect dry-run validation; "
                                        + "connectivity NOT verified"));
                return;
            }

            List<CatalogTable> inputCatalogTables =
                    resolveSinkInputTables(inputVertices, configIndex, factoryId);
            SupportSinkDryRunValidation validation = (SupportSinkDryRunValidation) factory;
            for (CatalogTable catalogTable : inputCatalogTables) {
                TableSinkFactoryContext context =
                        TableSinkFactoryContext.replacePlaceholderAndCreate(
                                catalogTable,
                                readonlyConfig,
                                classLoader,
                                factory.excludeTablePlaceholderReplaceKeys());
                validation.validateConnectionForDryRun(context);
            }
            // The exact checks (table existence, field compatibility, ...) are connector-specific,
            // so the framework only claims what it enforced: the connector hook ran and passed.
            results.add(
                    PluginResult.validated(
                            PluginType.SINK,
                            configIndex,
                            factoryId,
                            "connector dry-run connection validation passed"));
        } catch (Exception e) {
            throw wrap(PluginType.SINK, configIndex, factoryId, e);
        }
    }

    /**
     * Flattens sink input vertices to the catalog tables the sink must accept, enforcing the same
     * multi-input constraints as the runtime parser.
     */
    private List<CatalogTable> resolveSinkInputTables(
            List<SchemaInfo> inputVertices, int configIndex, String factoryId) {
        if (inputVertices.size() > 1) {
            for (SchemaInfo inputVertex : inputVertices) {
                if (inputVertex.getCatalogTables().size() > 1) {
                    throw new ConfigCheckException(
                            location(PluginType.SINK, configIndex, factoryId)
                                    + " does not support writing both a multi-table source and other sources.");
                }
            }
            List<CatalogTable> mergedInputs =
                    inputVertices.stream()
                            .map(SchemaInfo::getCatalogTables)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());
            checkCatalogTableTypesEqual(mergedInputs, PluginType.SINK, configIndex, factoryId);
            return Collections.singletonList(mergedInputs.get(0));
        }
        return inputVertices.get(0).getCatalogTables();
    }

    /**
     * Returns true only when the config declares a schema that actually defines columns, mirroring
     * the non-placeholder branches of {@code TableSchemaDiscoverer}. A bare {@code schema} block
     * without fields/columns would silently resolve to the synthetic single-text-column
     * placeholder, which must not be trusted for downstream schema checks.
     */
    private boolean hasExplicitSchema(ReadonlyConfig config) {
        if (config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            return declaresColumns(config);
        }
        if (config.getOptional(TableSchemaOptions.TABLE_CONFIGS).isPresent()) {
            return config.get(TableSchemaOptions.TABLE_CONFIGS).stream()
                    .map(ReadonlyConfig::fromMap)
                    .allMatch(this::declaresColumns);
        }
        if (config.getOptional(CatalogOptions.TABLE_LIST).isPresent()) {
            return config.get(CatalogOptions.TABLE_LIST).stream()
                    .map(ReadonlyConfig::fromMap)
                    .allMatch(this::declaresColumns);
        }
        return false;
    }

    private boolean declaresColumns(ReadonlyConfig entryConfig) {
        Map<String, Object> schemaMap = entryConfig.get(ConnectorCommonOptions.SCHEMA);
        if (schemaMap == null) {
            return false;
        }
        ReadonlyConfig schemaConfig = ReadonlyConfig.fromMap(schemaMap);
        return schemaConfig.getOptional(ColumnOptions.COLUMNS).isPresent()
                || entryConfig.getOptional(FieldOptions.FIELDS).isPresent()
                || schemaConfig.getOptional(ColumnOptions.METADATA_TABLE_ID).isPresent();
    }

    private void checkCatalogTableTypesEqual(
            List<CatalogTable> catalogTables,
            PluginType pluginType,
            int configIndex,
            String factoryId) {
        if (catalogTables.isEmpty()) {
            return;
        }
        CatalogTable expected = catalogTables.get(0);
        for (CatalogTable catalogTable : catalogTables) {
            if (!expected.getSeaTunnelRowType().equals(catalogTable.getSeaTunnelRowType())) {
                throw new ConfigCheckException(
                        location(pluginType, configIndex, factoryId)
                                + " does not support processing inputs with different schemas. "
                                + "Expected table "
                                + expected.getTableId()
                                + " but found table "
                                + catalogTable.getTableId()
                                + ".");
            }
        }
    }

    private List<String> getInputIds(ReadonlyConfig config) {
        return config.getOptional(PLUGIN_INPUT).orElse(Collections.singletonList(DEFAULT_ID));
    }

    private <T> T findLast(LinkedHashMap<?, T> map) {
        if (map.isEmpty()) {
            throw new ConfigCheckException(
                    "No upstream source or transform is available for sink.");
        }
        T result = null;
        for (T value : map.values()) {
            result = value;
        }
        return result;
    }

    private void logSummary(List<PluginResult> results) {
        StringBuilder summary = new StringBuilder("Dry-run connect validation summary:");
        for (PluginResult result : results) {
            summary.append(System.lineSeparator()).append("  ").append(result);
        }
        log.info(summary.toString());
    }

    private ConfigCheckException wrap(
            PluginType pluginType, int configIndex, String factoryId, Exception e) {
        String location = location(pluginType, configIndex, factoryId);
        String sanitizedMessage = DryRunConnectFailureMessageSanitizer.sanitize(e.getMessage());
        if (e instanceof ConfigCheckException
                && sanitizedMessage != null
                && sanitizedMessage.contains(location)) {
            return new ConfigCheckException(sanitizedMessage);
        }
        return new ConfigCheckException(location + " failed: " + sanitizedMessage);
    }

    private static String location(PluginType pluginType, int configIndex, String factoryId) {
        return String.format("%s[%d](%s)", pluginType.getType(), configIndex, factoryId);
    }

    /**
     * Schemas propagated through the DAG during connect dry-run. {@code unknown} means the source
     * could neither validate its schema nor read one from explicit config, so downstream schema
     * checks must be skipped instead of validated against a synthetic placeholder.
     */
    private static final class SchemaInfo {
        private final List<CatalogTable> catalogTables;

        private SchemaInfo(List<CatalogTable> catalogTables) {
            this.catalogTables = catalogTables;
        }

        private static SchemaInfo trusted(List<CatalogTable> catalogTables) {
            return new SchemaInfo(catalogTables);
        }

        private static SchemaInfo unknown() {
            return new SchemaInfo(null);
        }

        private boolean isUnknown() {
            return catalogTables == null;
        }

        private List<CatalogTable> getCatalogTables() {
            return catalogTables;
        }
    }

    /** Per-plugin outcome of the connect dry-run, surfaced to users in the summary log. */
    static final class PluginResult {
        enum Status {
            VALIDATED,
            SKIPPED
        }

        private final PluginType pluginType;
        private final int configIndex;
        private final String factoryId;
        private final Status status;
        private final String detail;

        private PluginResult(
                PluginType pluginType,
                int configIndex,
                String factoryId,
                Status status,
                String detail) {
            this.pluginType = pluginType;
            this.configIndex = configIndex;
            this.factoryId = factoryId;
            this.status = status;
            this.detail = detail;
        }

        private static PluginResult validated(
                PluginType pluginType, int configIndex, String factoryId, String detail) {
            return new PluginResult(pluginType, configIndex, factoryId, Status.VALIDATED, detail);
        }

        private static PluginResult skipped(
                PluginType pluginType, int configIndex, String factoryId, String detail) {
            return new PluginResult(pluginType, configIndex, factoryId, Status.SKIPPED, detail);
        }

        Status getStatus() {
            return status;
        }

        PluginType getPluginType() {
            return pluginType;
        }

        String getFactoryId() {
            return factoryId;
        }

        String getDetail() {
            return detail;
        }

        @Override
        public String toString() {
            return location(pluginType, configIndex, factoryId)
                    + ": "
                    + status
                    + " ("
                    + detail
                    + ")";
        }
    }
}
