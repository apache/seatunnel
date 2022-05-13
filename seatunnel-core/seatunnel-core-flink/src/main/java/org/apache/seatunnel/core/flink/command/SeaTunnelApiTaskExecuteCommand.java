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

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.core.base.command.Command;
import org.apache.seatunnel.core.base.config.ConfigBuilder;
import org.apache.seatunnel.core.base.config.EngineType;
import org.apache.seatunnel.core.base.exception.CommandExecuteException;
import org.apache.seatunnel.core.base.utils.FileUtils;
import org.apache.seatunnel.core.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.flink.FlinkEnvironment;
import org.apache.seatunnel.translation.flink.serialization.WrappedRow;
import org.apache.seatunnel.translation.flink.sink.FlinkSinkConverter;
import org.apache.seatunnel.translation.flink.source.SeaTunnelParallelSource;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
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
        // todo: add basic type
        SeaTunnelParallelSource source = getSource();
        Sink<WrappedRow, Object, Object, Object> flinkSink = getSink();
        // execute the flink job
        FlinkEnvironment flinkEnvironment = getFlinkEnvironment(config);
        StreamExecutionEnvironment streamExecutionEnvironment = flinkEnvironment.getStreamExecutionEnvironment();
        DataStreamSource<WrappedRow> dataStream = streamExecutionEnvironment.addSource(source);
        dataStream.sinkTo(flinkSink);
        try {
            streamExecutionEnvironment.execute("SeaTunnelAPITaskExecuteCommand");
        } catch (Exception e) {
            throw new CommandExecuteException("SeaTunnelAPITaskExecuteCommand execute failed", e);
        }
    }

    private SeaTunnelParallelSource getSource() {
        return new SeaTunnelParallelSource(loadSourcePlugin());
    }

    private Sink<WrappedRow, Object, Object, Object> getSink() {
        SeaTunnelSink<SeaTunnelRow, Object, Object, Object> sink = loadSinkPlugin();
        FlinkSinkConverter<SeaTunnelRow, WrappedRow, Object, Object, Object> flinkSinkConverter = new FlinkSinkConverter<>();
        return flinkSinkConverter.convert(sink, Collections.emptyMap());
    }

    private <T, SplitT extends SourceSplit, StateT> SeaTunnelSource<T, SplitT, StateT> loadSourcePlugin() {
        // todo: use FactoryUtils to load the plugin
        ServiceLoader<SeaTunnelSource> serviceLoader = ServiceLoader.load(SeaTunnelSource.class);
        Iterator<SeaTunnelSource> iterator = serviceLoader.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        throw new IllegalArgumentException("Cannot find the plugin.");
    }

    private <IN, StateT, CommitInfoT, AggregatedCommitInfoT> SeaTunnelSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT> loadSinkPlugin() {
        // todo: use FactoryUtils to load the plugin
        ServiceLoader<SeaTunnelSink> serviceLoader = ServiceLoader.load(SeaTunnelSink.class);
        Iterator<SeaTunnelSink> iterator = serviceLoader.iterator();
        if (iterator.hasNext()) {
            return iterator.next();
        }
        throw new IllegalArgumentException("Cannot find the plugin.");
    }

    private FlinkEnvironment getFlinkEnvironment(Config config) {
        FlinkEnvironment flinkEnvironment = new FlinkEnvironment();
        flinkEnvironment.setJobMode(JobMode.STREAMING);
        flinkEnvironment.setConfig(config);
        flinkEnvironment.prepare();

        return flinkEnvironment;
    }
}
