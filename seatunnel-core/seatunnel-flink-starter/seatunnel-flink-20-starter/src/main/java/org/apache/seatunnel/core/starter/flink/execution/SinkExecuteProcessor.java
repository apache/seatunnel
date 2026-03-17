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
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportSchemaEvolutionSink;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.flink.schema.BroadcastSchemaSinkOperator;
import org.apache.seatunnel.translation.flink.sink.FlinkSink;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;

import java.net.URL;
import java.util.List;

import static org.apache.seatunnel.common.constants.JobMode.STREAMING;

/** Sink execute processor for Flink 1.20. */
public class SinkExecuteProcessor extends AbstractSinkExecuteProcessor {

    protected SinkExecuteProcessor(
            List<URL> jarPaths,
            Config envConfig,
            List<? extends Config> pluginConfigs,
            JobContext jobContext) {
        super(jarPaths, envConfig, pluginConfigs, jobContext);
    }

    @Override
    protected DataStreamSink<SeaTunnelRow> createVersionSpecificDataStreamSink(
            DataStreamTableInfo stream, SeaTunnelSink sink, int parallelism, Config sinkConfig) {
        boolean isStreaming =
                envConfig.hasPath("job.mode")
                        && STREAMING.toString().equalsIgnoreCase(envConfig.getString("job.mode"));
        DataStream<SeaTunnelRow> ds = stream.getDataStream();
        if (isStreaming && sink instanceof SupportSchemaEvolutionSink) {
            // insert broadcast-based schema operator to handle schema changes
            ds =
                    ds.transform(
                                    "BroadcastSchemaHandler",
                                    TypeInformation.of(SeaTunnelRow.class),
                                    new BroadcastSchemaSinkOperator())
                            .name("BroadcastSchemaHandler")
                            .setParallelism(parallelism);
        }
        return ds.sinkTo(new FlinkSink<>(sink, stream.getCatalogTables(), parallelism))
                .name(String.format("%s-Sink", sink.getPluginName()));
    }
}
