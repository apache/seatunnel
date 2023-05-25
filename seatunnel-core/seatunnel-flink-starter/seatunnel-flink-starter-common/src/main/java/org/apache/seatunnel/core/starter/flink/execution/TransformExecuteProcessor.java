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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.core.starter.exception.TaskExecuteException;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelTransformPluginDiscovery;
import org.apache.seatunnel.translation.flink.serialization.FlinkRowConverter;
import org.apache.seatunnel.translation.flink.utils.TypeConverterUtils;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import com.google.common.collect.Lists;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class TransformExecuteProcessor
        extends FlinkAbstractPluginExecuteProcessor<SeaTunnelTransform> {

    private static final String PLUGIN_TYPE = "transform";

    protected TransformExecuteProcessor(
            List<URL> jarPaths, List<? extends Config> pluginConfigs, JobContext jobContext) {
        super(jarPaths, pluginConfigs, jobContext);
    }

    @Override
    protected List<SeaTunnelTransform> initializePlugins(
            List<URL> jarPaths, List<? extends Config> pluginConfigs) {
        SeaTunnelTransformPluginDiscovery transformPluginDiscovery =
                new SeaTunnelTransformPluginDiscovery();
        List<URL> pluginJars = new ArrayList<>();
        List<SeaTunnelTransform> transforms =
                pluginConfigs.stream()
                        .map(
                                transformConfig -> {
                                    PluginIdentifier pluginIdentifier =
                                            PluginIdentifier.of(
                                                    ENGINE_TYPE,
                                                    PLUGIN_TYPE,
                                                    transformConfig.getString(PLUGIN_NAME));
                                    List<URL> pluginJarPaths =
                                            transformPluginDiscovery.getPluginJarPaths(
                                                    Lists.newArrayList(pluginIdentifier));
                                    SeaTunnelTransform<?> seaTunnelTransform =
                                            transformPluginDiscovery.createPluginInstance(
                                                    pluginIdentifier);
                                    jarPaths.addAll(pluginJarPaths);
                                    seaTunnelTransform.prepare(transformConfig);
                                    seaTunnelTransform.setJobContext(jobContext);
                                    return seaTunnelTransform;
                                })
                        .distinct()
                        .collect(Collectors.toList());
        jarPaths.addAll(pluginJars);
        return transforms;
    }

    @Override
    public List<DataStream<Row>> execute(List<DataStream<Row>> upstreamDataStreams)
            throws TaskExecuteException {
        if (plugins.isEmpty()) {
            return upstreamDataStreams;
        }
        DataStream<Row> input = upstreamDataStreams.get(0);
        List<DataStream<Row>> result = new ArrayList<>();
        for (int i = 0; i < plugins.size(); i++) {
            try {
                SeaTunnelTransform<SeaTunnelRow> transform = plugins.get(i);
                Config pluginConfig = pluginConfigs.get(i);
                DataStream<Row> stream = fromSourceTable(pluginConfig).orElse(input);
                input = flinkTransform(transform, stream);
                registerResultTable(pluginConfig, input);
                result.add(input);
            } catch (Exception e) {
                throw new TaskExecuteException(
                        String.format(
                                "SeaTunnel transform task: %s execute error",
                                plugins.get(i).getPluginName()),
                        e);
            }
        }
        return result;
    }

    protected DataStream<Row> flinkTransform(SeaTunnelTransform transform, DataStream<Row> stream) {
        SeaTunnelDataType seaTunnelDataType = TypeConverterUtils.convert(stream.getType());
        transform.setTypeInfo(seaTunnelDataType);
        TypeInformation rowTypeInfo = TypeConverterUtils.convert(transform.getProducedType());
        FlinkRowConverter transformInputRowConverter = new FlinkRowConverter(seaTunnelDataType);
        FlinkRowConverter transformOutputRowConverter =
                new FlinkRowConverter(transform.getProducedType());
        DataStream<Row> output =
                stream.flatMap(
                        new FlatMapFunction<Row, Row>() {
                            @Override
                            public void flatMap(Row value, Collector<Row> out) throws Exception {
                                SeaTunnelRow seaTunnelRow =
                                        transformInputRowConverter.reconvert(value);
                                SeaTunnelRow dataRow = (SeaTunnelRow) transform.map(seaTunnelRow);
                                if (dataRow != null) {
                                    Row copy = transformOutputRowConverter.convert(dataRow);
                                    out.collect(copy);
                                }
                            }
                        },
                        rowTypeInfo);
        return output;
    }
}
