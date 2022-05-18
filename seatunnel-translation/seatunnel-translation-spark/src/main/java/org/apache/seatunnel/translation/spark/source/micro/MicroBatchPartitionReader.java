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
import org.apache.seatunnel.api.state.CheckpointLock;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.source.ParallelSource;
import org.apache.seatunnel.translation.spark.source.InternalRowCollector;
import org.apache.seatunnel.translation.spark.source.ReaderState;
import org.apache.seatunnel.translation.spark.source.batch.BatchPartitionReader;
import org.apache.seatunnel.translation.util.ThreadPoolExecutorFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MicroBatchPartitionReader extends BatchPartitionReader {
    protected static final Integer CHECKPOINT_SLEEP_INTERVAL = 10;
    protected volatile Integer checkpointId;

    protected final List<byte[]> restoredState;
    protected final CheckpointLock checkpointLock = new SingleReaderCheckpointLock();
    protected final Integer checkpointInterval;
    protected ScheduledThreadPoolExecutor executor;

    public MicroBatchPartitionReader(SeaTunnelSource<SeaTunnelRow, ?, ?> source, Integer parallelism, Integer subtaskId, Integer checkpointId, Integer checkpointInterval, List<byte[]> restoredState) {
        super(source, parallelism, subtaskId);
        this.checkpointId = checkpointId;
        this.restoredState = restoredState;
        this.checkpointInterval = checkpointInterval;
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
        return new InternalRowCollector(handover, checkpointLock);
    }

    @Override
    protected void prepare() {
        super.prepare();
        prepareCheckpoint();
    }

    protected ReaderState snapshotState() {
        List<byte[]> bytes;
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
        checkpointLock.lock();
        try {
            synchronized (collector) {
                while (!handover.isEmpty()) {
                    Thread.sleep(CHECKPOINT_SLEEP_INTERVAL);
                }
                // Block #next() method
                synchronized (handover) {
                    final int currentCheckpoint = checkpointId;
                    ReaderState readerState = snapshotState();
                    // TODO: save state

                    parallelSource.notifyCheckpointComplete(currentCheckpoint);
                    running = false;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("An error occurred in virtual checkpoint execution.", e);
        } finally {
            checkpointLock.unlock();
        }
    }

    @Override
    public void close() throws IOException {
        executor.shutdown();
        super.close();
    }
}
