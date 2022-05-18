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

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.core.base.command.Command;
import org.apache.seatunnel.core.base.config.ConfigBuilder;
import org.apache.seatunnel.core.base.config.EngineType;
import org.apache.seatunnel.core.base.config.EnvironmentFactory;
import org.apache.seatunnel.core.base.exception.CommandExecuteException;
import org.apache.seatunnel.core.base.utils.FileUtils;
import org.apache.seatunnel.core.spark.args.SparkCommandArgs;
import org.apache.seatunnel.plugin.discovery.PluginIdentifier;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSinkPluginDiscovery;
import org.apache.seatunnel.plugin.discovery.seatunnel.SeaTunnelSourcePluginDiscovery;
import org.apache.seatunnel.spark.SparkEnvironment;
import org.apache.seatunnel.translation.spark.sink.SparkSinkInjector;
import org.apache.seatunnel.translation.spark.utils.TypeConverterUtils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.HashMap;

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
        Path configFile = FileUtils.getConfigPath(sparkCommandArgs);

        Config config = new ConfigBuilder(configFile).getConfig();
        SparkEnvironment sparkEnvironment = getSparkEnvironment(config);
        SeaTunnelSource<?, ?, ?> source = getSource(config);
        Dataset<Row> dataset = sparkEnvironment.getSparkSession().read().format("SeaTunnelSource")
                .option("source.serialization", SerializationUtils.objectToString(source))
                .schema(TypeConverterUtils.convertRow(source.getRowTypeInfo())).load();
        SeaTunnelSink<?, ?, ?, ?> sink = getSink(config);
        try {
            SparkSinkInjector.inject(dataset.write(), sink, new HashMap<>(Common.COLLECTION_SIZE)).option(
                    "checkpointLocation", "/tmp").save();
        } catch (Exception e) {
            LOGGER.error("run seatunnel on spark failed.", e);
        }
    }

    private SeaTunnelSource<?, ?, ?> getSource(Config config) {
        PluginIdentifier pluginIdentifier = getSourcePluginIdentifier();
        // todo: use FactoryUtils to load the plugin
        SeaTunnelSourcePluginDiscovery sourcePluginDiscovery = new SeaTunnelSourcePluginDiscovery();
        return sourcePluginDiscovery.getPluginInstance(pluginIdentifier);
    }

    private SeaTunnelSink<?, ?, ?, ?> getSink(Config config) {
        PluginIdentifier pluginIdentifier = getSinkPluginIdentifier();
        SeaTunnelSinkPluginDiscovery sinkPluginDiscovery = new SeaTunnelSinkPluginDiscovery();
        return sinkPluginDiscovery.getPluginInstance(pluginIdentifier);
    }

    private PluginIdentifier getSourcePluginIdentifier() {
        return PluginIdentifier.of("seatunnel", "source", "FakeSource");
    }

    private PluginIdentifier getSinkPluginIdentifier() {
        return PluginIdentifier.of("seatunnel", "sink", "Console");
    }

    private SparkEnvironment getSparkEnvironment(Config config) {
        SparkEnvironment sparkEnvironment = (SparkEnvironment) new EnvironmentFactory<>(config, EngineType.SPARK).getEnvironment();
        sparkEnvironment.setJobMode(JobMode.STREAMING);
        sparkEnvironment.setConfig(config);
        sparkEnvironment.prepare();

        return sparkEnvironment;
    }
}
