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

package org.apache.seatunnel.core.flink.execution;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.translation.flink.sink.FlinkSinkConverter;
import org.apache.seatunnel.translation.flink.utils.TypeConverterUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import scala.Serializable;

public class SinkExecuteProcessor extends AbstractPluginExecuteProcessor<SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>> {

    protected SinkExecuteProcessor(FlinkEnvironment flinkEnvironment,
                                   List<? extends Config> pluginConfigs) {
        super(flinkEnvironment, pluginConfigs);
    }

    @Override
    protected List<SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>> initializePlugins(List<? extends Config> pluginConfigs) {
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        List<URL> pluginJars = new ArrayList<>();
        List<SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>> sinks = pluginConfigs.stream().map(sinkConfig -> {
            PluginIdentifier pluginIdentifier = PluginIdentifier.of(
                "seatunnel",
                "sink",
                sinkConfig.getString("plugin_name"));
            pluginJars.addAll(sinkPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier)));
            return (SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable>) sinkPluginDiscovery.getPluginInstance(pluginIdentifier);
        }).collect(Collectors.toList());
        flinkEnvironment.registerPlugin(pluginJars);
        return sinks;
    }

    @Override
    public List<DataStream<Row>> execute(List<DataStream<Row>> upstreamDataStreams) throws Exception {
        DataStream<Row> input = upstreamDataStreams.get(0);
        FlinkSinkConverter<SeaTunnelRow, Row, Serializable, Serializable, Serializable> flinkSinkConverter = new FlinkSinkConverter<>();
        for (int i = 0; i < plugins.size(); i++) {
            Config sinkConfig = pluginConfigs.get(i);
            SeaTunnelSink<SeaTunnelRow, Serializable, Serializable, Serializable> seaTunnelSink = plugins.get(i);
            DataStream<Row> stream = fromSourceTable(sinkConfig).orElse(input);
            seaTunnelSink.setTypeInfo(getSeaTunnelRowTypeInfo(stream));
            stream.sinkTo(flinkSinkConverter.convert(seaTunnelSink, Collections.emptyMap()));
        }
        // the sink is the last stream
        return null;
    }

    private SeaTunnelRowTypeInfo getSeaTunnelRowTypeInfo(DataStream<Row> stream) {
        RowTypeInfo typeInformation = (RowTypeInfo) stream.getType();
        String[] fieldNames = typeInformation.getFieldNames();
        SeaTunnelDataType<?>[] seaTunnelDataTypes = Arrays.stream(typeInformation.getFieldTypes())
            .map(TypeConverterUtils::convertType).toArray(SeaTunnelDataType[]::new);
        return new SeaTunnelRowTypeInfo(fieldNames, seaTunnelDataTypes);
    }
}
