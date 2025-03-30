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
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.file.sink.commit.FileSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.file.sink.state.FileSinkState;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.AbstractWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.WriteStrategy;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.DEFAULT_FILE_NAME_EXPRESSION;
import static org.apache.seatunnel.connectors.seatunnel.file.config.BaseSinkConfig.FILE_NAME_EXPRESSION;

public class BaseFileSinkWriter
        implements SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState>,
                SupportMultiTableSinkWriter<WriteStrategy> {

    protected final WriteStrategy writeStrategy;

    public BaseFileSinkWriter(
            WriteStrategy writeStrategy,
            HadoopConf hadoopConf,
            SinkWriter.Context context,
            String jobId,
            List<FileSinkState> fileSinkStates) {
        this.writeStrategy = writeStrategy;
        int subTaskIndex = context.getIndexOfSubtask();
        String uuidPrefix;
        if (!fileSinkStates.isEmpty()) {
            uuidPrefix = fileSinkStates.get(0).getUuidPrefix();
        } else {
            uuidPrefix = UUID.randomUUID().toString().replaceAll("-", "").substring(0, 10);
        }
        writeStrategy.init(hadoopConf, jobId, uuidPrefix, subTaskIndex);
        final HadoopFileSystemProxy hadoopFileSystemProxy =
                writeStrategy.getHadoopFileSystemProxy();
        if (!fileSinkStates.isEmpty()) {
            try {
                List<String> transactions =
                        findTransactionList(jobId, uuidPrefix, hadoopFileSystemProxy);
                FileSinkAggregatedCommitter fileSinkAggregatedCommitter =
                        new FileSinkAggregatedCommitter(hadoopConf);
                fileSinkAggregatedCommitter.init();
                LinkedHashMap<String, FileSinkState> fileStatesMap = new LinkedHashMap<>();
                fileSinkStates.forEach(
                        fileSinkState ->
                                fileStatesMap.put(fileSinkState.getTransactionId(), fileSinkState));
                for (String transaction : transactions) {
                    if (fileStatesMap.containsKey(transaction)) {
                        // need commit
                        FileSinkState fileSinkState = fileStatesMap.get(transaction);
                        FileAggregatedCommitInfo fileCommitInfo =
                                fileSinkAggregatedCommitter.combine(
                                        Collections.singletonList(
                                                new FileCommitInfo(
                                                        fileSinkState.getNeedMoveFiles(),
                                                        fileSinkState.getPartitionDirAndValuesMap(),
                                                        fileSinkState.getTransactionDir())));
                        fileSinkAggregatedCommitter.commit(
                                Collections.singletonList(fileCommitInfo));
                    } else {
                        // need abort
                        writeStrategy.abortPrepare(transaction);
                    }
                }
            } catch (IOException e) {
                String errorMsg =
                        String.format("Try to process these fileStates %s failed", fileSinkStates);
                throw new FileConnectorException(
                        CommonErrorCodeDeprecated.WRITER_OPERATION_FAILED, errorMsg, e);
            }
            writeStrategy.beginTransaction(fileSinkStates.get(0).getCheckpointId() + 1);
        } else {
            writeStrategy.beginTransaction(1L);
        }
        preCheckConfig(context);
    }

    private void preCheckConfig(SinkWriter.Context context) {
        if (writeStrategy.getFileSinkConfig().isSingleFileMode()
                && context.getNumberOfParallelSubtasks() > 1) {
            if (StringUtils.isNotEmpty(writeStrategy.getFileSinkConfig().getFileNameExpression())
                    && !writeStrategy
                            .getFileSinkConfig()
                            .getFileNameExpression()
                            .contains(DEFAULT_FILE_NAME_EXPRESSION)) {
                throw new IllegalArgumentException(
                        "Single file mode is not supported when "
                                + FILE_NAME_EXPRESSION.key()
                                + " not contains "
                                + DEFAULT_FILE_NAME_EXPRESSION
                                + " but has parallel subtasks.");
            }
        }
    }

    private List<String> findTransactionList(
            String jobId, String uuidPrefix, HadoopFileSystemProxy hadoopFileSystemProxy)
            throws IOException {
        return hadoopFileSystemProxy
                .getAllSubFiles(
                        AbstractWriteStrategy.getTransactionDirPrefix(
                                writeStrategy.getFileSinkConfig().getTmpPath(), jobId, uuidPrefix))
                .stream()
                .map(Path::getName)
                .collect(Collectors.toList());
    }

    public BaseFileSinkWriter(
            WriteStrategy writeStrategy,
            HadoopConf hadoopConf,
            SinkWriter.Context context,
            String jobId) {
        this(writeStrategy, hadoopConf, context, jobId, Collections.emptyList());
        writeStrategy.beginTransaction(1L);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        try {
            writeStrategy.write(element);
        } catch (SeaTunnelRuntimeException e) {
            throw CommonError.writeSeaTunnelRowFailed("FileConnector", element.toString(), e);
        }
    }

    @Override
    public Optional<FileCommitInfo> prepareCommit() throws IOException {
        return writeStrategy.prepareCommit();
    }

    @Override
    public void abortPrepare() {
        writeStrategy.abortPrepare();
    }

    @Override
    public List<FileSinkState> snapshotState(long checkpointId) throws IOException {
        return writeStrategy.snapshotState(checkpointId);
    }

    @Override
    public void close() throws IOException {
        if (writeStrategy != null) {
            writeStrategy.close();
        }
    }
}
