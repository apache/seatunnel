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

package org.apache.seatunnel.translation.spark.sink.write;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportResourceShare;
import org.apache.seatunnel.api.sink.event.WriterCloseEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.Constants;
import org.apache.seatunnel.translation.spark.execution.CheckpointMetadata;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;
import org.apache.seatunnel.translation.spark.serialization.InternalRowConverter;
import org.apache.seatunnel.translation.spark.source.partition.micro.CheckpointDataLogManager;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.execution.streaming.CheckpointFileManager;
import org.apache.spark.sql.types.StructType;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

@Slf4j
public class SeaTunnelSparkDataWriter<CommitInfoT, StateT> implements DataWriter<InternalRow> {

    protected final SinkWriter<SeaTunnelRow, CommitInfoT, StateT> sinkWriter;

    @Nullable protected final SinkCommitter<CommitInfoT> sinkCommitter;
    protected CommitInfoT latestCommitInfoT;
    protected long epochId;
    protected volatile MultiTableResourceManager resourceManager;
    private final Map<String, String> properties;

    private final MultiTableManager multiTableManager;
    private final SinkWriter.Context context;
    private final StructType schema;
    private final Path checkpointLocation;
    private final int partitionId;

    private FileSystem fs;

    private CheckpointDataLogManager logManager = null;

    public SeaTunnelSparkDataWriter(
            SinkWriter<SeaTunnelRow, CommitInfoT, StateT> sinkWriter,
            @Nullable SinkCommitter<CommitInfoT> sinkCommitter,
            MultiTableManager multiTableManager,
            StructType schema,
            Map<String, String> properties,
            String checkpointLocation,
            int partitionId,
            long epochId,
            SinkWriter.Context context) {
        this.sinkWriter = sinkWriter;
        this.sinkCommitter = sinkCommitter;
        this.multiTableManager = multiTableManager;
        this.checkpointLocation = new Path(checkpointLocation, "sinks");
        this.partitionId = partitionId;
        this.epochId = epochId == 0 ? 1 : epochId;
        this.properties = properties;
        this.schema = schema;
        this.context = context;
        initResourceManger();
    }

    @Override
    public void write(InternalRow record) throws IOException {
        Map<String, String> metadata = InternalRowConverter.unpackMetadata(record);
        CheckpointMetadata checkpointMetadata = CheckpointMetadata.of(metadata);
        if (checkpointMetadata.isCheckpoint()) {
            String location = checkpointMetadata.location();
            int batchId = checkpointMetadata.batchId();
            int subTaskId = checkpointMetadata.subTaskId();
            int checkpointId = checkpointMetadata.checkpointId();
            ackCheckpoint(location, batchId, subTaskId, checkpointId);
            return;
        }
        sinkWriter.write(multiTableManager.reconvert(record));
    }

    private void ackCheckpoint(String location, int batchId, int subTaskId, int checkpointId) {
        try {
            Optional<CommitInfoT> commitInfoTOptional = sinkWriter.prepareCommit();
            sinkWriter.snapshotState(epochId++);
            if (logManager == null) {
                Configuration configuration = new Configuration();
                String hdfsRoot =
                        properties.getOrDefault(
                                Constants.HDFS_ROOT,
                                FileSystem.getDefaultUri(configuration).toString());
                String hdfsUser = properties.getOrDefault(Constants.HDFS_USER, "");
                this.logManager =
                        new CheckpointDataLogManager(
                                CheckpointFileManager.create(
                                        this.checkpointLocation, configuration));
            }
            if (fs == null) {
                fs = getFileSystem();
            }
            Path preCommitPath =
                    logManager.preparedCommittedFilePath(
                            location, batchId, subTaskId, checkpointId);
            if (!fs.exists(preCommitPath)) {
                throw new IllegalStateException(
                        String.format(
                                "The source's snapshot(%s)[batchId=%d,subTaskId=%d,checkpointId=%d] must be existed.",
                                preCommitPath.toString(), batchId, subTaskId, checkpointId));
            }

            if (!fs.getFileStatus(preCommitPath).isFile()) {
                throw new IllegalStateException(
                        String.format(
                                "The source's snapshot(%s)[batchId=%d,subTaskId=%d,checkpointId=%d] must be one file.",
                                preCommitPath.toString(), batchId, subTaskId, checkpointId));
            }

            Path ackCommitFilePath =
                    logManager.committedFilePath(location, batchId, subTaskId, checkpointId);
            fs.rename(preCommitPath, ackCommitFilePath);
        } catch (IOException | URISyntaxException | InterruptedException e) {
            throw new IllegalStateException(
                    String.format(
                            "Fail to ack the source's snapshot[batchId=%d,subTaskId=%d,checkpointId=%d].",
                            batchId, subTaskId, checkpointId),
                    e);
        }
    }

    private FileSystem getFileSystem()
            throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        String hdfsRoot =
                properties.getOrDefault(
                        Constants.HDFS_ROOT, FileSystem.getDefaultUri(configuration).toString());
        String hdfsUser = properties.getOrDefault(Constants.HDFS_USER, "");
        configuration.set("fs.defaultFS", hdfsRoot);
        if (StringUtils.isNotBlank(hdfsUser)) {
            return FileSystem.get(new URI(hdfsRoot), configuration, hdfsUser);
        } else {
            return FileSystem.get(new URI(hdfsRoot), configuration);
        }
    }

    protected void initResourceManger() {
        if (sinkWriter instanceof SupportResourceShare) {
            resourceManager =
                    ((SupportResourceShare) sinkWriter).initMultiTableResourceManager(1, 1);
            ((SupportResourceShare) sinkWriter).setMultiTableResourceManager(resourceManager, 0);
        }
    }

    @Override
    public WriterCommitMessage commit() throws IOException {
        Optional<CommitInfoT> commitInfoTOptional = sinkWriter.prepareCommit();
        commitInfoTOptional.ifPresent(commitInfoT -> latestCommitInfoT = commitInfoT);
        sinkWriter.snapshotState(epochId++);
        if (sinkCommitter != null) {
            if (latestCommitInfoT == null) {
                sinkCommitter.commit(Collections.emptyList());
            } else {
                sinkCommitter.commit(Collections.singletonList(latestCommitInfoT));
            }
        }
        SeaTunnelSparkWriterCommitMessage<CommitInfoT> seaTunnelSparkWriterCommitMessage =
                new SeaTunnelSparkWriterCommitMessage<>(latestCommitInfoT);
        cleanCommitInfo();
        sinkWriter.close();
        context.getEventListener().onEvent(new WriterCloseEvent());
        try {
            if (resourceManager != null) {
                resourceManager.close();
            }
        } catch (Throwable e) {
            log.error("close resourceManager error", e);
        }
        return seaTunnelSparkWriterCommitMessage;
    }

    @Override
    public void abort() throws IOException {
        sinkWriter.abortPrepare();
        if (sinkCommitter != null) {
            if (latestCommitInfoT == null) {
                sinkCommitter.abort(Collections.emptyList());
            } else {
                sinkCommitter.abort(Collections.singletonList(latestCommitInfoT));
            }
        }
        cleanCommitInfo();
        if (this.fs != null) {
            this.fs.close();
        }
        sinkWriter.close();
    }

    private void cleanCommitInfo() {
        latestCommitInfoT = null;
    }

    @Override
    public void close() throws IOException {
        if (fs != null) {
            this.fs.close();
        }
        if (sinkCommitter != null) {
            if (latestCommitInfoT == null) {
                sinkCommitter.commit(Collections.emptyList());
            } else {
                sinkCommitter.commit(Collections.singletonList(latestCommitInfoT));
            }
        }
        cleanCommitInfo();
    }
}
