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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.SaveMode;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.TextFileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.FileSystemCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.SinkFileSystemPlugin;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Hive Sink implementation by using SeaTunnel sink API.
 */
public abstract class AbstractFileSink
    implements SeaTunnelSink<SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo> {
    private Config config;
    private String jobId;
    private Long checkpointId;
    private SeaTunnelRowType seaTunnelRowTypeInfo;
    private JobContext jobContext;
    private TextFileSinkConfig textFileSinkConfig;
    private SinkFileSystemPlugin sinkFileSystemPlugin;

    public abstract SinkFileSystemPlugin getSinkFileSystemPlugin();

    @Override
    public String getPluginName() {
        this.sinkFileSystemPlugin = getSinkFileSystemPlugin();
        return this.sinkFileSystemPlugin.getPluginName();
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
    public SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> createWriter(SinkWriter.Context context)
        throws IOException {
        if (!jobContext.getJobMode().equals(JobMode.BATCH) &&
            this.getSinkConfig().getSaveMode().equals(SaveMode.OVERWRITE)) {
            throw new RuntimeException("only batch job can overwrite mode");
        }

        if (this.getSinkConfig().isEnableTransaction()) {
            return new TransactionStateFileSinkWriter(seaTunnelRowTypeInfo,
                config,
                context,
                getSinkConfig(),
                jobId,
                sinkFileSystemPlugin);
        } else {
            throw new RuntimeException("File Sink Connector only support transaction now");
        }
    }

    @Override
    public SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> restoreWriter(SinkWriter.Context context,
                                                                                 List<FileSinkState> states)
        throws IOException {
        if (this.getSinkConfig().isEnableTransaction()) {
            return new FileSinkWriterWithTransaction(seaTunnelRowTypeInfo,
                config,
                context,
                textFileSinkConfig,
                jobId,
                states,
                sinkFileSystemPlugin);
        } else {
            throw new RuntimeException("File Sink Connector only support transaction now");
        }
    }

    @Override
    public void setSeaTunnelContext(JobContext jobContext) {
        this.jobContext = jobContext;
        this.jobId = jobContext.getJobId();
    }

    @Override
    public Optional<SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo>> createAggregatedCommitter()
        throws IOException {
        if (this.getSinkConfig().isEnableTransaction()) {
            Optional<FileSystemCommitter> fileSystemCommitter = sinkFileSystemPlugin.getFileSystemCommitter();
            if (fileSystemCommitter.isPresent()) {
                return Optional.of(new FileSinkAggregatedCommitter(fileSystemCommitter.get()));
            } else {
                throw new RuntimeException("FileSystemCommitter is need");
            }
        } else {
            return Optional.empty();
        }
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

    private TextFileSinkConfig getSinkConfig() {
        if (this.textFileSinkConfig == null && this.seaTunnelRowTypeInfo != null && this.config != null) {
            this.textFileSinkConfig = new TextFileSinkConfig(config, seaTunnelRowTypeInfo);
        }
        return this.textFileSinkConfig;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return this.seaTunnelRowTypeInfo;
    }
}


