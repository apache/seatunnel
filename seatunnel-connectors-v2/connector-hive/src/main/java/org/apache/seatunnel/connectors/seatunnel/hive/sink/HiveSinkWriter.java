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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.sink.HdfsFileSinkPlugin;
import org.apache.seatunnel.connectors.seatunnel.file.sink.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.file.sink.spi.SinkFileSystemPlugin;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionStateFileWriter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.FileSinkPartitionDirNameGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.FileSinkTransactionFileNameGenerator;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class HiveSinkWriter implements SinkWriter<SeaTunnelRow, HiveCommitInfo, HiveSinkState> {
    private static final Logger LOGGER = LoggerFactory.getLogger(HiveSinkWriter.class);

    private SeaTunnelRowType seaTunnelRowTypeInfo;
    private Config pluginConfig;
    private Context context;
    private String jobId;

    private TransactionStateFileWriter fileWriter;

    private HiveSinkConfig hiveSinkConfig;

    public HiveSinkWriter(@NonNull SeaTunnelRowType seaTunnelRowTypeInfo,
                          @NonNull Config pluginConfig,
                          @NonNull SinkWriter.Context context,
                          @NonNull HiveSinkConfig hiveSinkConfig,
                          @NonNull String jobId) {
        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
        this.pluginConfig = pluginConfig;
        this.context = context;
        this.jobId = jobId;
        this.hiveSinkConfig = hiveSinkConfig;
        this.fileWriter = createFileWriter();

        fileWriter.beginTransaction(1L);
    }

    public HiveSinkWriter(@NonNull SeaTunnelRowType seaTunnelRowTypeInfo,
                          @NonNull Config pluginConfig,
                          @NonNull SinkWriter.Context context,
                          @NonNull HiveSinkConfig hiveSinkConfig,
                          @NonNull String jobId,
                          @NonNull List<HiveSinkState> hiveSinkStates) {
        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
        this.pluginConfig = pluginConfig;
        this.context = context;
        this.jobId = jobId;
        this.hiveSinkConfig = hiveSinkConfig;
        this.fileWriter = createFileWriter();

        // Rollback dirty transaction
        if (hiveSinkStates.size() > 0) {
            List<String> transactionAfter = fileWriter.getTransactionAfter(hiveSinkStates.get(0).getTransactionId());
            fileWriter.abortTransactions(transactionAfter);
        }
        fileWriter.beginTransaction(hiveSinkStates.get(0).getCheckpointId() + 1);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        fileWriter.write(element);
    }

    @Override
    public Optional<HiveCommitInfo> prepareCommit() throws IOException {
        Optional<FileCommitInfo> fileCommitInfoOptional = fileWriter.prepareCommit();
        if (fileCommitInfoOptional.isPresent()) {
            FileCommitInfo fileCommitInfo = fileCommitInfoOptional.get();
            return Optional.of(new HiveCommitInfo(fileCommitInfo, hiveSinkConfig.getHiveMetaUris(), this.hiveSinkConfig.getTable()));
        }
        return Optional.empty();
    }

    @Override
    public void close() throws IOException {
        fileWriter.finishAndCloseWriteFile();
    }

    @Override
    public List<HiveSinkState> snapshotState(long checkpointId) throws IOException {
        List<FileSinkState> fileSinkStates = fileWriter.snapshotState(checkpointId);
        if (!CollectionUtils.isEmpty(fileSinkStates)) {
            return fileSinkStates.stream().map(state ->
                    new HiveSinkState(state.getTransactionId(), state.getCheckpointId()))
                .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    @Override
    public void abortPrepare() {
        fileWriter.abortTransaction();
    }

    private TransactionStateFileWriter createFileWriter() {
        SinkFileSystemPlugin sinkFileSystemPlugin = new HdfsFileSinkPlugin();
        Optional<TransactionStateFileWriter> transactionStateFileWriterOpt = sinkFileSystemPlugin.getTransactionStateFileWriter(this.seaTunnelRowTypeInfo,
                getFilenameGenerator(),
                getPartitionDirNameGenerator(),
                this.hiveSinkConfig.getTextFileSinkConfig().getSinkColumnsIndexInRow(),
                this.hiveSinkConfig.getTextFileSinkConfig().getTmpPath(),
                this.hiveSinkConfig.getTextFileSinkConfig().getPath(),
                this.jobId,
                this.context.getIndexOfSubtask(),
                this.hiveSinkConfig.getTextFileSinkConfig().getFieldDelimiter(),
                this.hiveSinkConfig.getTextFileSinkConfig().getRowDelimiter(),
                sinkFileSystemPlugin.getFileSystem().get());
        if (!transactionStateFileWriterOpt.isPresent()) {
            throw new RuntimeException("A TransactionStateFileWriter is need");
        }
        return transactionStateFileWriterOpt.get();
    }

    private FileSinkTransactionFileNameGenerator getFilenameGenerator() {
        return new FileSinkTransactionFileNameGenerator(
                this.hiveSinkConfig.getTextFileSinkConfig().getFileFormat(),
                this.hiveSinkConfig.getTextFileSinkConfig().getFileNameExpression(),
                this.hiveSinkConfig.getTextFileSinkConfig().getFileNameTimeFormat());
    }

    private FileSinkPartitionDirNameGenerator getPartitionDirNameGenerator() {
        return new FileSinkPartitionDirNameGenerator(
                this.hiveSinkConfig.getTextFileSinkConfig().getPartitionFieldList(),
                this.hiveSinkConfig.getTextFileSinkConfig().getPartitionFieldsIndexInRow(),
                this.hiveSinkConfig.getTextFileSinkConfig().getPartitionDirExpression());
    }
}
