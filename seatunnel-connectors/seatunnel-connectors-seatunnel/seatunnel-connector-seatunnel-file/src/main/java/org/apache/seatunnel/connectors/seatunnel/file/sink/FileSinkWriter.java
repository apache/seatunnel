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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.TextFileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.transaction.TransactionStateFileWriter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.FileSinkPartitionDirNameGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.FileSinkTransactionFileNameGenerator;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.HdfsTxtTransactionStateFileWriter;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class FileSinkWriter implements SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState> {
    private static final Logger LOGGER = LoggerFactory.getLogger(FileSinkWriter.class);

    private SeaTunnelRowType seaTunnelRowTypeInfo;
    private Config pluginConfig;
    private Context context;
    private String jobId;

    private TransactionStateFileWriter fileWriter;

    private TextFileSinkConfig textFileSinkConfig;

    public FileSinkWriter(@NonNull SeaTunnelRowType seaTunnelRowTypeInfo,
                          @NonNull Config pluginConfig,
                          @NonNull SinkWriter.Context context,
                          @NonNull TextFileSinkConfig textFileSinkConfig,
                          @NonNull String jobId) {
        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
        this.pluginConfig = pluginConfig;
        this.context = context;
        this.jobId = jobId;
        this.textFileSinkConfig = textFileSinkConfig;

        fileWriter = new HdfsTxtTransactionStateFileWriter(this.seaTunnelRowTypeInfo,
            new FileSinkTransactionFileNameGenerator(
                this.textFileSinkConfig.getFileFormat(),
                this.textFileSinkConfig.getFileNameExpression(),
                this.textFileSinkConfig.getFileNameTimeFormat()),
            new FileSinkPartitionDirNameGenerator(
                this.textFileSinkConfig.getPartitionFieldList(),
                this.textFileSinkConfig.getPartitionFieldsIndexInRow(),
                this.textFileSinkConfig.getPartitionDirExpression()),
            this.textFileSinkConfig.getSinkColumnsIndexInRow(),
            this.textFileSinkConfig.getTmpPath(),
            this.textFileSinkConfig.getPath(),
            this.jobId,
            this.context.getIndexOfSubtask(),
            this.textFileSinkConfig.getFieldDelimiter(),
            this.textFileSinkConfig.getRowDelimiter());

        fileWriter.beginTransaction(1L);
    }

    public FileSinkWriter(@NonNull SeaTunnelRowType seaTunnelRowTypeInfo,
                          @NonNull Config pluginConfig,
                          @NonNull SinkWriter.Context context,
                          @NonNull TextFileSinkConfig textFileSinkConfig,
                          @NonNull String jobId,
                          @NonNull List<FileSinkState> fileSinkStates) {
        this.seaTunnelRowTypeInfo = seaTunnelRowTypeInfo;
        this.pluginConfig = pluginConfig;
        this.context = context;
        this.jobId = jobId;

        fileWriter = new HdfsTxtTransactionStateFileWriter(this.seaTunnelRowTypeInfo,
            new FileSinkTransactionFileNameGenerator(
                this.textFileSinkConfig.getFileFormat(),
                this.textFileSinkConfig.getFileNameExpression(),
                this.textFileSinkConfig.getFileNameTimeFormat()),
            new FileSinkPartitionDirNameGenerator(
                this.textFileSinkConfig.getPartitionFieldList(),
                this.textFileSinkConfig.getPartitionFieldsIndexInRow(),
                this.textFileSinkConfig.getPartitionDirExpression()),
            this.textFileSinkConfig.getSinkColumnsIndexInRow(),
            this.textFileSinkConfig.getTmpPath(),
            this.textFileSinkConfig.getPath(),
            this.jobId,
            this.context.getIndexOfSubtask(),
            this.textFileSinkConfig.getFieldDelimiter(),
            this.textFileSinkConfig.getRowDelimiter());

        // Rollback dirty transaction
        if (fileSinkStates.size() > 0) {
            List<String> transactionAfter = fileWriter.getTransactionAfter(fileSinkStates.get(0).getTransactionId());
            fileWriter.abortTransactions(transactionAfter);
        }
        fileWriter.beginTransaction(fileSinkStates.get(0).getCheckpointId() + 1);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        fileWriter.write(element);
    }

    @Override
    public Optional<FileCommitInfo> prepareCommit() throws IOException {
        return fileWriter.prepareCommit();
    }

    @Override
    public void abortPrepare() {
        fileWriter.abortTransaction();
    }

    @Override
    public void close() throws IOException {
        fileWriter.finishAndCloseWriteFile();
    }

    @Override
    public List<FileSinkState> snapshotState(long checkpointId) throws IOException {
        List<FileSinkState> fileSinkStates = fileWriter.snapshotState(checkpointId);
        fileWriter.beginTransaction(checkpointId);
        return fileSinkStates;
    }
}
