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

package org.apache.seatunnel.core.spark.command;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.core.base.command.Command;
import org.apache.seatunnel.core.base.config.ConfigBuilder;
import org.apache.seatunnel.core.base.config.EngineType;
import org.apache.seatunnel.core.base.exception.CommandExecuteException;
import org.apache.seatunnel.core.base.utils.FileUtils;
import org.apache.seatunnel.core.spark.args.SparkCommandArgs;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Lists;
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
public class SeaTunnelApiTaskExecuteCommand implements Command<SparkCommandArgs> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SeaTunnelApiTaskExecuteCommand.class);

    private final SparkCommandArgs sparkCommandArgs;

    public SeaTunnelApiTaskExecuteCommand(SparkCommandArgs sparkCommandArgs) {
        this.sparkCommandArgs = sparkCommandArgs;
    }

    @Override
    public void execute() throws CommandExecuteException {
        EngineType engine = sparkCommandArgs.getEngineType();
        Path configFile = FileUtils.getConfigPath(sparkCommandArgs);

        Config config = new ConfigBuilder(configFile).getConfig();
        SeaTunnelParallelSource source = getSource(config);
        // todo: add basic type
        Sink<WrappedRow, Object, Object, Object> flinkSink = getSink(config);

        FlinkEnvironment flinkEnvironment = getFlinkEnvironment(config);
        registerPlugins(flinkEnvironment);

        StreamExecutionEnvironment streamExecutionEnvironment = flinkEnvironment.getStreamExecutionEnvironment();
        // support multiple sources/sink
        DataStreamSource<WrappedRow> dataStream = streamExecutionEnvironment.addSource(source);
        // todo: add transform
        dataStream.sinkTo(flinkSink);
        try {
            streamExecutionEnvironment.execute("SeaTunnelAPITaskExecuteCommand");
        } catch (Exception e) {
            throw new CommandExecuteException("SeaTunnelAPITaskExecuteCommand execute failed", e);
        }
    }

    private SeaTunnelParallelSource getSource(Config config) {
        PluginIdentifier pluginIdentifier = getSourcePluginIdentifier();
        // todo: use FactoryUtils to load the plugin
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();
        return new SeaTunnelParallelSource(sourcePluginDiscovery.getPluginInstance(pluginIdentifier));
    }

    private Sink<WrappedRow, Object, Object, Object> getSink(Config config) {
        PluginIdentifier pluginIdentifier = getSinkPluginIdentifier();
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        FlinkSinkConverter<SeaTunnelRow, WrappedRow, Object, Object, Object> flinkSinkConverter = new FlinkSinkConverter<>();
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
        flinkEnvironment.setConfig(config);
        flinkEnvironment.prepare();

        return flinkEnvironment;
    }
}
