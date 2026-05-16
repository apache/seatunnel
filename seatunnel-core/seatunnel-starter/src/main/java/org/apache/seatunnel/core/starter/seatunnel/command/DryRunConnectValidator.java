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
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.FactoryUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.starter.exception.ConfigCheckException;
import org.apache.seatunnel.engine.core.parse.ConfigParserUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_INPUT;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_OUTPUT;
import static org.apache.seatunnel.api.table.factory.FactoryUtil.DEFAULT_ID;

/**
 * Performs Layer 1 dry-run validation without creating source/sink runtime objects, readers,
 * writers, committers, or save-mode handlers.
 */
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

    void validate() {
        ClassLoader parentClassLoader = Thread.currentThread().getContextClassLoader();
        LinkedHashMap<String, List<CatalogTable>> tableWithCatalogTables = new LinkedHashMap<>();

        try {
            Thread.currentThread().setContextClassLoader(sourceAndTransformClassLoader);
            for (int configIndex = 0; configIndex < sourceConfigs.size(); configIndex++) {
                Config sourceConfig = sourceConfigs.get(configIndex);
                SourceDryRunResult source =
                        validateSource(configIndex, sourceConfig, sourceAndTransformClassLoader);
                tableWithCatalogTables.put(source.tableId, source.catalogTables);
            }

            validateTransforms(
                    transformConfigs, sourceAndTransformClassLoader, tableWithCatalogTables);

            Thread.currentThread().setContextClassLoader(sinkClassLoader);
            for (int configIndex = 0; configIndex < sinkConfigs.size(); configIndex++) {
                validateSink(
                        configIndex,
                        sinkConfigs.get(configIndex),
                        sinkClassLoader,
                        tableWithCatalogTables);
            }
        } finally {
            Thread.currentThread().setContextClassLoader(parentClassLoader);
        }
    }

    private SourceDryRunResult validateSource(
            int configIndex, Config sourceConfig, ClassLoader classLoader) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(sourceConfig);
        String factoryId = ConfigParserUtil.getFactoryId(readonlyConfig);
        try {
            TableSourceFactory factory =
                    FactoryUtil.discoverFactory(classLoader, TableSourceFactory.class, factoryId);
            TableSourceFactoryContext context =
                    new TableSourceFactoryContext(readonlyConfig, classLoader);
            List<CatalogTable> catalogTables = factory.inferSchemaForDryRun(context);
            if (catalogTables == null || catalogTables.isEmpty()) {
                throw new ConfigCheckException(
                        location(PluginType.SOURCE, configIndex, factoryId)
                                + " did not infer any source schema.");
            }
            factory.validateConnectionForDryRun(context, catalogTables);
            return new SourceDryRunResult(
                    readonlyConfig.getOptional(PLUGIN_OUTPUT).orElse(DEFAULT_ID), catalogTables);
        } catch (Exception e) {
            throw wrap(PluginType.SOURCE, configIndex, factoryId, e);
        }
    }

    private void validateTransforms(
            List<? extends Config> configs,
            ClassLoader classLoader,
            LinkedHashMap<String, List<CatalogTable>> tableWithCatalogTables) {
        if (configs.isEmpty()) {
            return;
        }
        Queue<Config> remainingTransforms = new LinkedList<>(configs);
        int index = 0;
        while (!remainingTransforms.isEmpty()) {
            Config transformConfig = remainingTransforms.poll();
            if (!validateTransform(
                    index++,
                    transformConfig,
                    remainingTransforms,
                    classLoader,
                    tableWithCatalogTables)) {
                remainingTransforms.offer(transformConfig);
            }
        }
    }

    private boolean validateTransform(
            int configIndex,
            Config transformConfig,
            Queue<Config> remainingTransforms,
            ClassLoader classLoader,
            LinkedHashMap<String, List<CatalogTable>> tableWithCatalogTables) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(transformConfig);
        String factoryId = ConfigParserUtil.getFactoryId(readonlyConfig);
        try {
            List<CatalogTable> inputCatalogTables =
                    getInputIds(readonlyConfig).stream()
                            .map(tableWithCatalogTables::get)
                            .filter(Objects::nonNull)
                            .flatMap(Collection::stream)
                            .collect(Collectors.toList());
            if (inputCatalogTables.isEmpty()) {
                if (remainingTransforms.isEmpty()) {
                    inputCatalogTables = findLast(tableWithCatalogTables);
                } else {
                    return false;
                }
            }
            checkCatalogTableTypesEqual(
                    inputCatalogTables, PluginType.TRANSFORM, configIndex, factoryId);
            SeaTunnelTransform<?> transform =
                    FactoryUtil.createAndPrepareMultiTableTransform(
                            new ArrayList<>(new LinkedHashSet<>(inputCatalogTables)),
                            readonlyConfig,
                            classLoader,
                            factoryId);
            tableWithCatalogTables.put(
                    readonlyConfig.getOptional(PLUGIN_OUTPUT).orElse(DEFAULT_ID),
                    transform.getProducedCatalogTables());
            return true;
        } catch (Exception e) {
            throw wrap(PluginType.TRANSFORM, configIndex, factoryId, e);
        }
    }

    private void validateSink(
            int configIndex,
            Config sinkConfig,
            ClassLoader classLoader,
            LinkedHashMap<String, List<CatalogTable>> tableWithCatalogTables) {
        ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(sinkConfig);
        String factoryId = ConfigParserUtil.getFactoryId(readonlyConfig);
        try {
            TableSinkFactory factory =
                    FactoryUtil.discoverFactory(classLoader, TableSinkFactory.class, factoryId);
            List<List<CatalogTable>> inputVertices =
                    getInputIds(readonlyConfig).stream()
                            .map(tableWithCatalogTables::get)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());

            if (inputVertices.isEmpty()) {
                inputVertices = Collections.singletonList(findLast(tableWithCatalogTables));
            } else if (inputVertices.size() > 1) {
                for (List<CatalogTable> inputVertex : inputVertices) {
                    if (inputVertex.size() > 1) {
                        throw new ConfigCheckException(
                                location(PluginType.SINK, configIndex, factoryId)
                                        + " does not support writing both a multi-table source and other sources.");
                    }
                }
            }

            if (inputVertices.size() > 1) {
                List<CatalogTable> mergedInputs =
                        inputVertices.stream()
                                .flatMap(Collection::stream)
                                .collect(Collectors.toList());
                checkCatalogTableTypesEqual(mergedInputs, PluginType.SINK, configIndex, factoryId);
                validateSinkFactory(factory, mergedInputs.get(0), readonlyConfig, classLoader);
                return;
            }

            for (CatalogTable catalogTable : inputVertices.get(0)) {
                validateSinkFactory(factory, catalogTable, readonlyConfig, classLoader);
            }
        } catch (Exception e) {
            throw wrap(PluginType.SINK, configIndex, factoryId, e);
        }
    }

    private void validateSinkFactory(
            TableSinkFactory factory,
            CatalogTable catalogTable,
            ReadonlyConfig readonlyConfig,
            ClassLoader classLoader)
            throws Exception {
        TableSinkFactoryContext context =
                TableSinkFactoryContext.replacePlaceholderAndCreate(
                        catalogTable,
                        readonlyConfig,
                        classLoader,
                        factory.excludeTablePlaceholderReplaceKeys());
        factory.validateConnectionForDryRun(context);
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

    private ConfigCheckException wrap(
            PluginType pluginType, int configIndex, String factoryId, Exception e) {
        if (e instanceof ConfigCheckException
                && e.getMessage() != null
                && e.getMessage().contains(location(pluginType, configIndex, factoryId))) {
            return (ConfigCheckException) e;
        }
        return new ConfigCheckException(
                location(pluginType, configIndex, factoryId) + " failed: " + e.getMessage(), e);
    }

    private String location(PluginType pluginType, int configIndex, String factoryId) {
        return String.format("%s[%d](%s)", pluginType.getType(), configIndex, factoryId);
    }

    private static class SourceDryRunResult {
        private final String tableId;
        private final List<CatalogTable> catalogTables;

        private SourceDryRunResult(String tableId, List<CatalogTable> catalogTables) {
            this.tableId = tableId;
            this.catalogTables = catalogTables;
        }
    }
}
