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

package org.apache.seatunnel.translation.spark.source.micro;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.translation.source.ParallelSource;
import org.apache.seatunnel.translation.spark.source.InternalRowCollector;
import org.apache.seatunnel.translation.spark.source.ReaderState;
import org.apache.seatunnel.translation.spark.source.batch.BatchPartitionReader;
import org.apache.seatunnel.translation.util.ThreadPoolExecutorFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.StructType;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MicroBatchPartitionReader extends BatchPartitionReader {
    protected static final Integer CHECKPOINT_SLEEP_INTERVAL = 10;
    protected volatile Integer checkpointId;

    protected final Object checkpointLock = new Object();
    protected final Integer checkpointInterval;
    protected final String checkpointPath;
    protected final String hdfsRoot;
    protected final String hdfsUser;

    protected Map<Integer, List<byte[]>> restoredState;
    protected ScheduledThreadPoolExecutor executor;
    protected FileSystem fileSystem;

    public MicroBatchPartitionReader(SeaTunnelSource<SeaTunnelRow, ?, ?> source,
                                     Integer parallelism,
                                     Integer subtaskId,
                                     StructType rowType,
                                     Integer checkpointId,
                                     Integer checkpointInterval,
                                     String checkpointPath,
                                     String hdfsRoot,
                                     String hdfsUser) {
        super(source, parallelism, subtaskId, rowType);
        this.checkpointId = checkpointId;
        this.checkpointInterval = checkpointInterval;
        this.checkpointPath = checkpointPath;
        this.hdfsRoot = hdfsRoot;
        this.hdfsUser = hdfsUser;
    }

    @Override
    protected ParallelSource<SeaTunnelRow, ?, ?> createParallelSource() {
        return new InternalParallelSource<>(source,
            restoredState,
            parallelism,
            subtaskId);
    }

    @Override
    protected Collector<SeaTunnelRow> createCollector() {
        return new InternalRowCollector(handover, checkpointLock, rowType);
    }

    @Override
    protected void prepare() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsRoot);
        try {
            this.fileSystem = FileSystem.get(new URI(hdfsRoot), configuration, hdfsUser);
            this.restoredState = restoreState(checkpointId - 1);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        super.prepare();
        prepareCheckpoint();
    }

    protected ReaderState snapshotState() {
        Map<Integer, List<byte[]>> bytes;
        try {
            bytes = parallelSource.snapshotState(checkpointId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return new ReaderState(bytes, subtaskId, checkpointId++);
    }

    public void prepareCheckpoint() {
        executor = ThreadPoolExecutorFactory.createScheduledThreadPoolExecutor(1, String.format("parallel-reader-checkpoint-executor-%s", subtaskId));
        executor.schedule(this::virtualCheckpoint, checkpointInterval, TimeUnit.MILLISECONDS);
    }

    public void virtualCheckpoint() {
        try {
            synchronized (checkpointLock) {
                while (!handover.isEmpty()) {
                    Thread.sleep(CHECKPOINT_SLEEP_INTERVAL);
                }
                // Block #next() method
                synchronized (handover) {
                    final int currentCheckpoint = checkpointId;
                    ReaderState readerState = snapshotState();
                    saveState(readerState, currentCheckpoint);
                    parallelSource.notifyCheckpointComplete(currentCheckpoint);
                    running = false;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("An error occurred in virtual checkpoint execution.", e);
        }
    }

    private Map<Integer, List<byte[]>> restoreState(int checkpointId) throws IOException {
        Path hdfsPath = getCheckpointPathWithId(checkpointId);
        if (!fileSystem.exists(hdfsPath)) {
            return null;
        }
        try (FSDataInputStream inputStream = fileSystem.open(hdfsPath);
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

    private void saveState(ReaderState readerState, int checkpointId) throws IOException {
        byte[] bytes = SerializationUtils.serialize(readerState);
        Path hdfsPath = getCheckpointPathWithId(checkpointId);
        if (!fileSystem.exists(hdfsPath)) {
            fileSystem.createNewFile(hdfsPath);
        }

        try (FSDataOutputStream outputStream = fileSystem.append(hdfsPath)) {
            outputStream.write(bytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Path getCheckpointPathWithId(int checkpointId) {
        return new Path(this.checkpointPath + File.separator + this.subtaskId + File.separator + checkpointId);
    }

    @Override
    public void close() throws IOException {
        fileSystem.close();
        executor.shutdown();
        super.close();
    }
}
