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

package org.apache.seatunnel.connectors.seatunnel.file.sink;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.SaveMode;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.TextFileSinkConfig;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Hive Sink implementation by using SeaTunnel sink API.
 * This class contains the method to create {@link FileSinkWriter} and {@link FileSinkAggregatedCommitter}.
 */
@AutoService(SeaTunnelSink.class)
public class FileSink implements SeaTunnelSink<SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo> {
    private Config config;
    private String jobId;
    private Long checkpointId;
    private SeaTunnelRowType seaTunnelRowTypeInfo;
    private SeaTunnelContext seaTunnelContext;
    private TextFileSinkConfig textFileSinkConfig;

    @Override
    public String getPluginName() {
        return "File";
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowTypeInfo) {
        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.config = pluginConfig;
        this.checkpointId = 1L;
    }

    @Override
    public SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> createWriter(SinkWriter.Context context) throws IOException {
        this.textFileSinkConfig = new TextFileSinkConfig(config, seaTunnelRowTypeInfo);
        if (!seaTunnelContext.getJobMode().equals(JobMode.BATCH) && textFileSinkConfig.getSaveMode().equals(SaveMode.OVERWRITE)) {
            throw new RuntimeException("only batch job can overwrite mode");
        }
        return new FileSinkWriter(seaTunnelRowTypeInfo, config, context, textFileSinkConfig, jobId);
    }

    @Override
    public SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> restoreWriter(SinkWriter.Context context, List<FileSinkState> states) throws IOException {
        return new FileSinkWriter(seaTunnelRowTypeInfo, config, context, textFileSinkConfig, jobId, states);
    }

    @Override
    public SeaTunnelContext getSeaTunnelContext() {
        return this.seaTunnelContext;
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
        this.jobId = seaTunnelContext.getJobId();
    }

    @Override
    public Optional<SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo>> createAggregatedCommitter() throws IOException {
        return Optional.of(new FileSinkAggregatedCommitter());
    }

    @Override
    public Optional<Serializer<FileSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<FileAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<FileCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }
}


