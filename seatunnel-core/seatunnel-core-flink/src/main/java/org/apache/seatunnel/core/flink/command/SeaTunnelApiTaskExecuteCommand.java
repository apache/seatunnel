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

package org.apache.seatunnel.core.flink.command;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.core.base.command.Command;
import org.apache.seatunnel.core.base.config.ConfigBuilder;
import org.apache.seatunnel.core.base.config.EngineType;
import org.apache.seatunnel.core.base.exception.CommandExecuteException;
import org.apache.seatunnel.core.base.utils.FileUtils;
import org.apache.seatunnel.core.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.flink.util.TableUtil;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.translation.flink.sink.FlinkSinkConverter;
import org.apache.seatunnel.translation.flink.source.SeaTunnelParallelSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * todo: do we need to move these class to a new module? since this may cause version conflict with the old flink version.
 * This command is used to execute the Flink job by SeaTunnel new API.
 */
public class SeaTunnelApiTaskExecuteCommand implements Command<FlinkCommandArgs> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkApiConfValidateCommand.class);

    private final FlinkCommandArgs flinkCommandArgs;

    public SeaTunnelApiTaskExecuteCommand(FlinkCommandArgs flinkCommandArgs) {
        this.flinkCommandArgs = flinkCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException {
        EngineType engine = flinkCommandArgs.getEngineType();
        Path configFile = FileUtils.getConfigPath(flinkCommandArgs);

        Config config = new ConfigBuilder(configFile).getConfig();
        FlinkEnvironment flinkEnvironment = getFlinkEnvironment(config);

        SeaTunnelParallelSource source = getSource(config);
        // todo: add basic type
        Sink<Row, Object, Object, Object> flinkSink = getSink(config);

        registerPlugins(flinkEnvironment);

        StreamExecutionEnvironment streamExecutionEnvironment = flinkEnvironment.getStreamExecutionEnvironment();
        // support multiple sources/sink
        DataStreamSource<Row> dataStream = streamExecutionEnvironment.addSource(source);
        registerTable(dataStream, "fake_table", "name, age", flinkEnvironment);
        // todo: add transform
        DataStream<Row> transformOutputStream = TableUtil.tableToDataStream(
            flinkEnvironment.getStreamTableEnvironment(),
            flinkEnvironment.getStreamTableEnvironment().sqlQuery("select * from fake_table"),
            false);
        // add sink
        transformOutputStream.sinkTo(flinkSink);
        try {
            streamExecutionEnvironment.execute("SeaTunnelAPITaskExecuteCommand");
        } catch (Exception e) {
            throw new CommandExecuteException("SeaTunnelAPITaskExecuteCommand execute failed", e);
        }
    }

    private void registerTable(DataStream<Row> dataStream, String tableName, String fields, FlinkEnvironment flinkEnvironment) {
        StreamTableEnvironment streamTableEnvironment = flinkEnvironment.getStreamTableEnvironment();
        if (!TableUtil.tableExists(streamTableEnvironment, tableName)) {
            streamTableEnvironment.registerDataStream(tableName, dataStream, fields);
        }
    }

    private DataStream<Row> fromSourceTable(FlinkEnvironment flinkEnvironment, String tableName) {
        StreamTableEnvironment streamTableEnvironment = flinkEnvironment.getStreamTableEnvironment();
        Table fakeTable = streamTableEnvironment.scan(tableName);
        return TableUtil.tableToDataStream(streamTableEnvironment, fakeTable, true);
    }

    private SeaTunnelParallelSource getSource(Config config) {
        PluginIdentifier pluginIdentifier = getSourcePluginIdentifier();
        // todo: use FactoryUtils to load the plugin
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();
        return new SeaTunnelParallelSource(sourcePluginDiscovery.getPluginInstance(pluginIdentifier));
    }

    private Sink<Row, Object, Object, Object> getSink(Config config) {
        PluginIdentifier pluginIdentifier = getSinkPluginIdentifier();
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        FlinkSinkConverter<SeaTunnelRow, Row, Object, Object, Object> flinkSinkConverter = new FlinkSinkConverter<>();
        return flinkSinkConverter.convert(sinkPluginDiscovery.getPluginInstance(pluginIdentifier), Collections.emptyMap());
    }

    private List<URL> getSourPluginJars(PluginIdentifier pluginIdentifier) {
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();
        return sourcePluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier));
    }

    private List<URL> getSinkPluginJars(PluginIdentifier pluginIdentifier) {
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        return sinkPluginDiscovery.getPluginJarPaths(Lists.newArrayList(pluginIdentifier));
    }

    private PluginIdentifier getSourcePluginIdentifier() {
        return PluginIdentifier.of("seatunnel", "source", "FakeSource");
    }

    private PluginIdentifier getSinkPluginIdentifier() {
        return PluginIdentifier.of("seatunnel", "sink", "Console");
    }

    private void registerPlugins(FlinkEnvironment flinkEnvironment) {
        List<URL> pluginJars = new ArrayList<>();
        pluginJars.addAll(getSourPluginJars(getSourcePluginIdentifier()));
        pluginJars.addAll(getSinkPluginJars(getSinkPluginIdentifier()));
        flinkEnvironment.registerPlugin(pluginJars);
    }

    private FlinkEnvironment getFlinkEnvironment(Config config) {
        FlinkEnvironment flinkEnvironment = new FlinkEnvironment();
        flinkEnvironment.setJobMode(JobMode.STREAMING);
        flinkEnvironment.setConfig(config.getConfig("env"));
        flinkEnvironment.prepare();

        return flinkEnvironment;
    }
}
