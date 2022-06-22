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

package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Hive Sink implementation by using SeaTunnel sink API.
 * This class contains the method to create {@link HiveSinkWriter} and {@link HiveSinkAggregatedCommitter}.
 */
@AutoService(SeaTunnelSink.class)
public class HiveSink implements SeaTunnelSink<SeaTunnelRow, HiveSinkState, HiveCommitInfo, HiveAggregatedCommitInfo> {

    private Config config;
    private long jobId;
    private SeaTunnelRowType seaTunnelRowType;

    @Override
    public String getPluginName() {
        return "Hive";
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.config = pluginConfig;
        this.jobId = System.currentTimeMillis();
    }

    @Override
    public SinkWriter<SeaTunnelRow, HiveCommitInfo, HiveSinkState> createWriter(SinkWriter.Context context) throws IOException {
        return new HiveSinkWriter(seaTunnelRowType, config, context, System.currentTimeMillis());
    }

    @Override
    public SinkWriter<SeaTunnelRow, HiveCommitInfo, HiveSinkState> restoreWriter(SinkWriter.Context context, List<HiveSinkState> states) throws IOException {
        return new HiveSinkWriter(seaTunnelRowType, config, context, System.currentTimeMillis());
    }

    @Override
    public SeaTunnelContext getSeaTunnelContext() {
        return null;
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {

    }

    @Override
    public Optional<Serializer<HiveCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkAggregatedCommitter<HiveCommitInfo, HiveAggregatedCommitInfo>> createAggregatedCommitter() throws IOException {
        return Optional.of(new HiveSinkAggregatedCommitter());
    }

    @Override
    public Optional<Serializer<HiveAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }
}
