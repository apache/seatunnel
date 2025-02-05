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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.core.starter.execution.PluginUtil;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelFactoryDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.common.CommonOptions.PLUGIN_OUTPUT;

@SuppressWarnings("unchecked,rawtypes")
public class TransformExecuteProcessor
        extends FlinkAbstractPluginExecuteProcessor<TableTransformFactory> {

    protected TransformExecuteProcessor(
            List<URL> jarPaths,
            Config envConfig,
            List<? extends Config> pluginConfigs,
            JobContext jobContext) {
        super(jarPaths, envConfig, pluginConfigs, jobContext);
    }

    @Override
    protected List<TableTransformFactory> initializePlugins(
            List<URL> jarPaths, List<? extends Config> pluginConfigs) {

        SeaTunnelFactoryDiscovery factoryDiscovery =
                new SeaTunnelFactoryDiscovery(TableTransformFactory.class, ADD_URL_TO_CLASSLOADER);
        SeaTunnelTransformPluginDiscovery transformPluginDiscovery =
                new SeaTunnelTransformPluginDiscovery();
        return pluginConfigs.stream()
                .map(
                        transformConfig ->
                                PluginUtil.createTransformFactory(
                                        factoryDiscovery,
                                        transformPluginDiscovery,
                                        transformConfig,
                                        jarPaths))
                .distinct()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(e -> (TableTransformFactory) e)
                .collect(Collectors.toList());
    }

    @Override
    public List<DataStreamTableInfo> execute(List<DataStreamTableInfo> upstreamDataStreams)
            throws TaskExecuteException {
        if (plugins.isEmpty()) {
            return upstreamDataStreams;
        }
        DataStreamTableInfo input = upstreamDataStreams.get(0);
        Map<String, DataStreamTableInfo> outputTables =
                upstreamDataStreams.stream()
                        .collect(
                                Collectors.toMap(
                                        DataStreamTableInfo::getTableName,
                                        e -> e,
                                        (a, b) -> b,
                                        LinkedHashMap::new));

        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        for (int i = 0; i < plugins.size(); i++) {
            try {
                Config pluginConfig = pluginConfigs.get(i);
                DataStreamTableInfo stream =
                        fromSourceTable(pluginConfig, new ArrayList<>(outputTables.values()))
                                .orElse(input);
                TableTransformFactory factory = plugins.get(i);
                TableTransformFactoryContext context =
                        new TableTransformFactoryContext(
                                stream.getCatalogTables(),
                                ReadonlyConfig.fromConfig(pluginConfig),
                                classLoader);
                ConfigValidator.of(context.getOptions()).validate(factory.optionRule());
                SeaTunnelTransform transform = factory.createTransform(context).createTransform();

                transform.setJobContext(jobContext);
                DataStream<SeaTunnelRow> inputStream =
                        flinkTransform(transform, stream.getDataStream());
                String pluginOutputIdentifier =
                        ReadonlyConfig.fromConfig(pluginConfig).get(PLUGIN_OUTPUT);
                // TODO transform support multi tables
                outputTables.put(
                        pluginOutputIdentifier,
                        new DataStreamTableInfo(
                                inputStream,
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

    protected DataStream<SeaTunnelRow> flinkTransform(
            SeaTunnelTransform transform, DataStream<SeaTunnelRow> stream) {
        if (transform instanceof SeaTunnelFlatMapTransform) {
            return stream.flatMap(
                    new ArrayFlatMap(transform), TypeInformation.of(SeaTunnelRow.class));
        }

        return stream.transform(
                        String.format("%s-Transform", transform.getPluginName()),
                        TypeInformation.of(SeaTunnelRow.class),
                        new StreamMap<>(
                                flinkRuntimeEnvironment
                                        .getStreamExecutionEnvironment()
                                        .clean(
                                                row ->
                                                        ((SeaTunnelMapTransform<SeaTunnelRow>)
                                                                        transform)
                                                                .map(row))))
                // null value shouldn't be passed to downstream
                .filter(Objects::nonNull);
    }

    public static class ArrayFlatMap implements FlatMapFunction<SeaTunnelRow, SeaTunnelRow> {

        private SeaTunnelTransform transform;

        public ArrayFlatMap(SeaTunnelTransform transform) {
            this.transform = transform;
        }

        @Override
        public void flatMap(SeaTunnelRow row, Collector<SeaTunnelRow> collector) {
            List<SeaTunnelRow> rows =
                    ((SeaTunnelFlatMapTransform<SeaTunnelRow>) transform).flatMap(row);
            if (CollectionUtils.isNotEmpty(rows)) {
                for (SeaTunnelRow rowResult : rows) {
                    collector.collect(rowResult);
                }
            }
        }
    }
}
