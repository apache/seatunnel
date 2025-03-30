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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategyFactory;

import java.util.List;
import java.util.Optional;

public abstract class BaseFileSink
        implements SeaTunnelSink<
                SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo> {
    protected SeaTunnelRowType seaTunnelRowType;
    protected Config pluginConfig;
    protected HadoopConf hadoopConf;
    protected FileSinkConfig fileSinkConfig;
    protected JobContext jobContext;
    protected String jobId;

    public void preCheckConfig() {
        if (pluginConfig.hasPath(BaseSinkConfig.SINGLE_FILE_MODE.key())
                && pluginConfig.getBoolean(BaseSinkConfig.SINGLE_FILE_MODE.key())
                && jobContext.isEnableCheckpoint()) {
            throw new IllegalArgumentException(
                    "Single file mode is not supported when checkpoint is enabled or in streaming mode.");
        }
        if (pluginConfig.hasPath(BaseSinkConfig.CREATE_EMPTY_FILE_WHEN_NO_DATA.key())
                && pluginConfig.getBoolean(BaseSinkConfig.CREATE_EMPTY_FILE_WHEN_NO_DATA.key())
                && !fileSinkConfig.getPartitionFieldList().isEmpty()) {
            throw new IllegalArgumentException(
                    "Generate empty file when no data is not supported when partition is enabled.");
        }
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
        this.jobId = jobContext.getJobId();
        preCheckConfig();
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.fileSinkConfig = new FileSinkConfig(pluginConfig, seaTunnelRowType);
    }

    @Override
    public SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> restoreWriter(
            SinkWriter.Context context, List<FileSinkState> states) {
        return new BaseFileSinkWriter(createWriteStrategy(), hadoopConf, context, jobId, states);
    }

    @Override
    public Optional<SinkAggregatedCommitter<FileCommitInfo, FileAggregatedCommitInfo>>
            createAggregatedCommitter() {
        return Optional.of(new FileSinkAggregatedCommitter(hadoopConf));
    }

    @Override
    public SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> createWriter(
            SinkWriter.Context context) {
        return new BaseFileSinkWriter(createWriteStrategy(), hadoopConf, context, jobId);
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

    /**
     * Use the pluginConfig to do some initialize operation.
     *
     * @param pluginConfig plugin config.
     * @throws PrepareFailException if plugin prepare failed, the {@link PrepareFailException} will
     *     throw.
     */
    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.pluginConfig = pluginConfig;
    }

    protected WriteStrategy createWriteStrategy() {
        WriteStrategy writeStrategy =
                WriteStrategyFactory.of(fileSinkConfig.getFileFormat(), fileSinkConfig);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable(
                        "file", null, null, TablePath.DEFAULT.getTableName(), seaTunnelRowType));
        return writeStrategy;
    }
}
