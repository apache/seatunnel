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

package org.apache.seatunnel.connectors.seatunnel.file.local.sink;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.local.config.LocalFileHadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.local.sink.writter.LocalFileSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategyFactory;

import java.util.List;
import java.util.Optional;

public class LocalFileSink
        implements SeaTunnelSink<
                        SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo>,
                SupportMultiTableSink {

    private final HadoopConf hadoopConf;
    private final HadoopFileSystemProxy hadoopFileSystemProxy;
    private final FileSinkConfig fileSinkConfig;
    private final WriteStrategy writeStrategy;
    private String jobId;

    public LocalFileSink(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        this.hadoopConf = new LocalFileHadoopConf();
        this.fileSinkConfig =
                new FileSinkConfig(readonlyConfig.toConfig(), catalogTable.getSeaTunnelRowType());
        this.writeStrategy =
                WriteStrategyFactory.of(
                        fileSinkConfig.getFileFormat(), fileSinkConfig, getPluginName());
        this.hadoopFileSystemProxy = new HadoopFileSystemProxy(hadoopConf);
        this.writeStrategy.setSeaTunnelRowTypeInfo(catalogTable.getSeaTunnelRowType());
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobId = jobContext.getJobId();
    }

    @Override
    public SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> restoreWriter(
            SinkWriter.Context context, List<FileSinkState> states) {
        return new LocalFileSinkWriter(writeStrategy, hadoopConf, context, jobId, states);
    }

    @Override
    public Optional<SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo>>
            createAggregatedCommitter() {
        return Optional.of(new FileSinkAggregatedCommitter(hadoopConf));
    }

    @Override
    public SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> createWriter(
            SinkWriter.Context context) {
        return new LocalFileSinkWriter(writeStrategy, hadoopConf, context, jobId);
    }

    @Override
    public Optional<Serializer<FileCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<FileAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<Serializer<FileSinkState>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public String getPluginName() {
        return FileSystemType.LOCAL.getFileSystemPluginName();
    }
}
