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

package org.apache.seatunnel.translation.spark.source.partition.micro;

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.Handover;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.translation.source.BaseSourceFunction;
import org.apache.seatunnel.translation.spark.execution.CheckpointMetadata;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;
import org.apache.seatunnel.translation.spark.serialization.InternalRowCollector;
import org.apache.seatunnel.translation.spark.serialization.InternalRowConverter;
import org.apache.seatunnel.translation.util.ThreadPoolExecutorFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.streaming.CheckpointFileManager;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ParallelMicroBatchPartitionReader {
    protected static final Integer CHECKPOINT_SLEEP_INTERVAL = 10;
    protected static final Integer CHECKPOINT_RETRIES = 3;
    protected volatile Integer batchId;
    protected final Integer checkpointInterval;
    protected final String checkpointPath;
    protected final String hdfsRoot;
    protected final String hdfsUser;

    protected Map<Integer, List<byte[]>> restoredState;
    protected ScheduledThreadPoolExecutor snapshotExecutor;
    protected FileSystem fileSystem;

    protected static final Integer INTERVAL = 1000;

    protected final SeaTunnelSource<SeaTunnelRow, ?, ?> source;
    protected final Integer parallelism;
    protected final String jobId;
    protected final Integer subtaskId;

    protected final ExecutorService collectorExecutor;
    protected final Handover<InternalRow> handover;

    protected final Object checkpointLock = new Object();

    protected volatile boolean running = true;
    protected volatile boolean prepared = false;

    protected volatile BaseSourceFunction<SeaTunnelRow> internalSource;
    protected volatile InternalRowCollector internalRowCollector;
    private final Map<String, String> envOptions;

    private final MultiTableManager multiTableManager;
    private volatile int checkpointId = 0;
    private final CheckpointDataLogManager logManager;
    private volatile boolean shouldCheckpoint = false;
    private volatile long totalCollectRowSize = 0;

    public ParallelMicroBatchPartitionReader(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            Integer parallelism,
            String jobId,
            Integer subtaskId,
            Integer batchId,
            Integer checkpointInterval,
            String checkpointPath,
            String hdfsRoot,
            String hdfsUser,
            Map<String, String> envOptions,
            MultiTableManager multiTableManager) {
        this.source = source;
        this.parallelism = parallelism;
        this.jobId = jobId;
        this.subtaskId = subtaskId;
        this.collectorExecutor =
                ThreadPoolExecutorFactory.createScheduledThreadPoolExecutor(
                        1, getEnumeratorThreadName());
        this.handover = new Handover<>();
        this.envOptions = envOptions;
        this.multiTableManager = multiTableManager;
        this.batchId = batchId;
        this.checkpointInterval = checkpointInterval;
        this.checkpointPath = checkpointPath;
        this.hdfsRoot = hdfsRoot;
        this.hdfsUser = hdfsUser;
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsRoot);
        this.logManager =
                new CheckpointDataLogManager(
                        CheckpointFileManager.create(new Path(this.checkpointPath), configuration));
    }

    protected String getEnumeratorThreadName() {
        return String.format("parallel-split-enumerator-executor-%s", subtaskId);
    }

    protected BaseSourceFunction<SeaTunnelRow> createInternalSource() {
        return new InternalParallelMicroBatchSource<>(
                this, source, restoredState, parallelism, jobId, subtaskId);
    }

    protected void prepare() {
        if (prepared) {
            return;
        }
        try {
            this.fileSystem = getFileSystem();
            int committedCheckpointId = this.latestCommittedCheckpointId(this.batchId);
            this.checkpointId = committedCheckpointId + 1;
            Path latestPreCommittedPath =
                    logManager.preparedCommittedFilePath(
                            this.checkpointPath, batchId, subtaskId, this.checkpointId);
            this.restoredState = restoreState(latestPreCommittedPath);
            if (this.restoredState == null) {
                /*
                 * If fail to restore state from the latest prepared checkpoint at the current batch, it will try restore state from the committed checkpoint.
                 */
                Path latestCommittedPath =
                        logManager.committedFilePath(
                                this.checkpointPath, batchId, subtaskId, committedCheckpointId);
                this.restoredState = restoreState(latestCommittedPath);
            }

            int prevBatchId = batchId;
            while (this.restoredState == null & --prevBatchId >= 0) {
                /*
                 * If fail to restore state from the latest committed checkpoint at the current batch, it will try restore state from the previous committed batch.
                 */
                int prevBatchCommittedCheckpointId = this.latestCommittedCheckpointId(prevBatchId);
                int prevBatchPreparedCheckpointId = this.latestPreparedCheckpointId(prevBatchId);
                Path recentCheckpointPath = null;
                if (prevBatchPreparedCheckpointId > prevBatchCommittedCheckpointId) {
                    recentCheckpointPath =
                            logManager.preparedCommittedFilePath(
                                    this.checkpointPath,
                                    prevBatchId,
                                    subtaskId,
                                    prevBatchPreparedCheckpointId);
                } else {
                    recentCheckpointPath =
                            logManager.committedFilePath(
                                    this.checkpointPath,
                                    prevBatchId,
                                    subtaskId,
                                    prevBatchCommittedCheckpointId);
                }
                this.restoredState = restoreState(recentCheckpointPath);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        this.internalSource = createInternalSource();
        try {
            this.internalSource.open();
        } catch (Exception e) {
            running = false;
            throw new RuntimeException("Failed to open internal source.", e);
        }

        this.internalRowCollector =
                multiTableManager.getInternalRowCollector(handover, checkpointLock, envOptions);
        collectorExecutor.execute(
                () -> {
                    try {
                        internalSource.run(internalRowCollector);
                    } catch (Exception e) {
                        handover.reportError(e);
                        log.error("BatchPartitionReader execute failed.", e);
                        running = false;
                    }
                });
        startCheckpointConditionService();
        prepared = true;
    }

    protected FileSystem getFileSystem()
            throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsRoot);
        if (StringUtils.isNotBlank(hdfsUser)) {
            return FileSystem.get(new URI(hdfsRoot), configuration, hdfsUser);
        } else {
            return FileSystem.get(new URI(hdfsRoot), configuration);
        }
    }

    protected ReaderState snapshotState() {
        Map<Integer, List<byte[]>> bytes;
        try {
            bytes = internalSource.snapshotState(checkpointId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new ReaderState(bytes, subtaskId, checkpointId++);
    }

    public void startCheckpointConditionService() {
        if (snapshotExecutor == null) {
            snapshotExecutor =
                    ThreadPoolExecutorFactory.createScheduledThreadPoolExecutor(
                            1,
                            String.format(
                                    "parallel-reader-checkpoint-condition-executor-%s-%s",
                                    batchId, subtaskId));
        }
        snapshotExecutor.schedule(
                this::checkpointCondition, checkpointInterval, TimeUnit.MILLISECONDS);
    }

    private void checkpointCondition() {
        long current = internalRowCollector.collectTotalCount();
        if (current > totalCollectRowSize) {
            shouldCheckpoint = true;
        }
        totalCollectRowSize = current;
    }

    private int latestCommittedCheckpointId(int batchId) throws IOException {
        Path path = logManager.subTaskLogDataPath(this.checkpointPath, batchId, this.subtaskId);
        return logManager.maxNum(path, Optional.of(CheckpointDataLogManager.SUFFIX_COMMITTED));
    }

    private int latestPreparedCheckpointId(int batchId) throws IOException {
        Path path = logManager.subTaskLogDataPath(this.checkpointPath, batchId, this.subtaskId);
        return logManager.maxNum(path, Optional.of(CheckpointDataLogManager.SUFFIX_PREPARED));
    }

    private Map<Integer, List<byte[]>> restoreState(Path statePath) throws IOException {
        if (!fileSystem.exists(statePath)) {
            return null;
        }
        try (FSDataInputStream inputStream = fileSystem.open(statePath);
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            int i = 0;
            final int defaultLen = 1024;
            byte[] buffer = new byte[defaultLen];
            while ((i = inputStream.read(buffer)) != -1) {
                out.write(buffer, 0, i);
            }

            return ((ReaderState) SerializationUtils.deserialize(out.toByteArray())).getBytes();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected void saveState(ReaderState readerState, int checkpointId) throws IOException {
        byte[] bytes = SerializationUtils.serialize(readerState);
        Path hdfsPath =
                logManager.preparedCommittedFilePath(
                        this.checkpointPath, batchId, this.subtaskId, checkpointId);

        if (!fileSystem.exists(hdfsPath)) {
            try (FSDataOutputStream outputStream = fileSystem.create(hdfsPath)) {
                outputStream.write(bytes);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return;
        }

        try (FSDataOutputStream outputStream = fileSystem.appendFile(hdfsPath).build()) {
            outputStream.write(bytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean next() throws IOException {
        prepare();
        if (!running) {
            return false;
        }

        if (shouldCheckpoint) {
            try {
                final int currentCheckpoint = checkpointId;
                ReaderState readerState = snapshotState();
                saveState(readerState, currentCheckpoint);
                internalSource.notifyCheckpointComplete(currentCheckpoint);
                InternalRow checkpointEvent =
                        checkpointEvent(batchId, subtaskId, currentCheckpoint);
                handover.produce(checkpointEvent);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                shouldCheckpoint = false;
            }
        }

        int retry = 20;
        while (retry-- > 0 & handover.isEmpty()) {
            try {
                Thread.sleep(INTERVAL);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        return !handover.isEmpty();
    }

    public InternalRow get() {
        try {
            return handover.pollNext().get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private InternalRow checkpointEvent(int batchId, int subtaskId, int checkpointId) {
        CatalogTable catalogTable = source.getProducedCatalogTables().get(0);
        return InternalRowConverter.checkpointEvent(
                catalogTable.getSeaTunnelRowType(),
                RowKind.INSERT,
                catalogTable.getTableId().toTablePath().toString(),
                CheckpointMetadata.of(this.checkpointPath, batchId, subtaskId, checkpointId));
    }

    public void close() throws IOException {
        try {
            snapshotExecutor.awaitTermination(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if (fileSystem != null) {
                fileSystem.close();
            }
            running = false;
            try {
                if (internalSource != null) {
                    internalSource.close();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            collectorExecutor.shutdown();
        }
    }

    public boolean isRunning() {
        return this.running;
    }

    public void setRunning(boolean status) {
        this.running = status;
    }

    public void virtualCheckpoint() {}
}
