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

package org.apache.seatunnel.core.starter.spark.execution;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.common.constants.EngineType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;
import org.apache.seatunnel.translation.spark.execution.DatasetTableInfo;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;

import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.GenericRow;

import lombok.extern.slf4j.Slf4j;

import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_NAME;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.PLUGIN_OUTPUT;

@Slf4j
public class TransformExecuteProcessor
        extends SparkAbstractPluginExecuteProcessor<TableTransformFactory> {

    protected TransformExecuteProcessor(
            SparkRuntimeEnvironment sparkRuntimeEnvironment,
            JobContext jobContext,
            List<? extends Config> pluginConfigs) {
        super(sparkRuntimeEnvironment, jobContext, pluginConfigs);
    }

    @Override
    protected List<TableTransformFactory> initializePlugins(List<? extends Config> pluginConfigs) {

        SeaTunnelTransformPluginDiscovery transformPluginDiscovery =
                new SeaTunnelTransformPluginDiscovery();

        SeaTunnelFactoryDiscovery factoryDiscovery =
                new SeaTunnelFactoryDiscovery(TableTransformFactory.class);

        List<URL> pluginJars = new ArrayList<>();
        List<TableTransformFactory> transforms =
                pluginConfigs.stream()
                        .map(
                                transformConfig -> {
                                    pluginJars.addAll(
                                            transformPluginDiscovery.getPluginJarPaths(
                                                    Lists.newArrayList(
                                                            PluginIdentifier.of(
                                                                    EngineType.SEATUNNEL
                                                                            .getEngine(),
                                                                    PluginType.TRANSFORM.getType(),
                                                                    transformConfig.getString(
                                                                            PLUGIN_NAME.key())))));
                                    return Optional.of(
                                            (TableTransformFactory)
                                                    factoryDiscovery.createPluginInstance(
                                                            PluginIdentifier.of(
                                                                    EngineType.SEATUNNEL
                                                                            .getEngine(),
                                                                    PluginType.TRANSFORM.getType(),
                                                                    transformConfig.getString(
                                                                            PLUGIN_NAME.key()))));
                                })
                        .distinct()
                        .map(Optional::get)
                        .collect(Collectors.toList());
        sparkRuntimeEnvironment.registerPlugin(pluginJars);
        return transforms;
    }

    @Override
    public List<DatasetTableInfo> execute(List<DatasetTableInfo> upstreamDataStreams)
            throws TaskExecuteException {
        if (plugins.isEmpty()) {
            return upstreamDataStreams;
        }
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        DatasetTableInfo input = upstreamDataStreams.get(0);

        Map<String, DatasetTableInfo> outputTables =
                upstreamDataStreams.stream()
                        .collect(
                                Collectors.toMap(
                                        DatasetTableInfo::getTableName,
                                        e -> e,
                                        (a, b) -> b,
                                        LinkedHashMap::new));
        for (int i = 0; i < plugins.size(); i++) {
            try {
                Config pluginConfig = pluginConfigs.get(i);
                DatasetTableInfo dataset =
                        fromSourceTable(
                                        pluginConfig,
                                        sparkRuntimeEnvironment,
                                        new ArrayList<>(outputTables.values()))
                                .orElse(input);
                TableTransformFactory factory = plugins.get(i);
                TableTransformFactoryContext context =
                        new TableTransformFactoryContext(
                                dataset.getCatalogTables(),
                                ReadonlyConfig.fromConfig(pluginConfig),
                                classLoader);
                ConfigValidator.of(context.getOptions()).validate(factory.optionRule());
                SeaTunnelTransform transform = factory.createTransform(context).createTransform();

                Dataset<Row> inputDataset = sparkTransform(transform, dataset);
                registerInputTempView(pluginConfig, inputDataset);
                String pluginOutputIdentifier =
                        ReadonlyConfig.fromConfig(pluginConfig).get(PLUGIN_OUTPUT);
                outputTables.put(
                        pluginOutputIdentifier,
                        new DatasetTableInfo(
                                inputDataset,
                                transform.getProducedCatalogTables(),
                                pluginOutputIdentifier));
            } catch (Exception e) {
                throw new TaskExecuteException(
                        String.format(
                                "SeaTunnel transform task: %s execute error",
                                plugins.get(i).factoryIdentifier()),
                        e);
            }
        }
        return new ArrayList<>(outputTables.values());
    }

    private Dataset<Row> sparkTransform(SeaTunnelTransform transform, DatasetTableInfo tableInfo) {
        MultiTableManager inputManager =
                new MultiTableManager(tableInfo.getCatalogTables().toArray(new CatalogTable[0]));
        MultiTableManager outputManager =
                new MultiTableManager(
                        (CatalogTable[])
                                transform.getProducedCatalogTables().toArray(new CatalogTable[0]));
        Dataset<Row> stream = tableInfo.getDataset();
        ExpressionEncoder<Row> encoder = RowEncoder.apply(outputManager.getTableSchema());
        return stream.flatMap(
                        new TransformMapPartitionsFunction(transform, inputManager, outputManager),
                        encoder)
                .filter(Objects::nonNull);
    }

    private static class TransformMapPartitionsFunction implements FlatMapFunction<Row, Row> {
        private SeaTunnelTransform<SeaTunnelRow> transform;
        private MultiTableManager inputManager;
        private MultiTableManager outputManager;

        public TransformMapPartitionsFunction(
                SeaTunnelTransform<SeaTunnelRow> transform,
                MultiTableManager inputManager,
                MultiTableManager outputManager) {
            this.transform = transform;
            this.inputManager = inputManager;
            this.outputManager = outputManager;
        }

        @Override
        public Iterator<Row> call(Row row) throws Exception {
            List<Row> rows = new ArrayList<>();

            SeaTunnelRow seaTunnelRow = inputManager.reconvert((GenericRow) row);
            if (transform instanceof SeaTunnelFlatMapTransform) {
                List<SeaTunnelRow> seaTunnelRows =
                        ((SeaTunnelFlatMapTransform<SeaTunnelRow>) transform).flatMap(seaTunnelRow);
                if (CollectionUtils.isNotEmpty(seaTunnelRows)) {
                    for (SeaTunnelRow seaTunnelRowTransform : seaTunnelRows) {
                        rows.add(outputManager.convert(seaTunnelRowTransform));
                    }
                }
            } else if (transform instanceof SeaTunnelMapTransform) {
                SeaTunnelRow seaTunnelRowTransform =
                        ((SeaTunnelMapTransform<SeaTunnelRow>) transform).map(seaTunnelRow);
                if (seaTunnelRowTransform != null) {
                    rows.add(outputManager.convert(seaTunnelRowTransform));
                }
            }
            return rows.iterator();
        }
    }
}
